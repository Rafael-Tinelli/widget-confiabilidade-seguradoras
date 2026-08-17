from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.sources.receita_cnpj import load_lifecycle_records
from api.sources.ses import extract_ses_master_and_financials
from api.sources.susep_financial_evidence import load_susep_financial_evidence
from api.sources.susep_groups import load_susep_economic_groups
from api.sources.susep_licensed import fetch_licensed_entities
from api.sources.susep_sandbox import fetch_sandbox_participants
from api.sources.susep_special_regimes import fetch_special_regime_records
from api.v2.build_classification_inventory import build_classification_inventory
from api.v2.build_eligibility_inventory import build_eligibility_inventory
from api.v2.build_lifecycle_relationship_inventory import (
    build_lifecycle_relationship_inventory,
)
from api.v2.financial_evidence import (
    apply_financial_evidence,
    financial_evidence_summary,
    validate_financial_evidence,
)
from api.v2.relationships import load_verified_relationship_registry

DEFAULT_OUTPUT = Path("data/derived/v2/entity_financial_evidence_inventory.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_financial_evidence_inventory(
    eligibility_payload: dict[str, Any],
    source_payload: dict[str, Any],
) -> dict[str, Any]:
    entities = apply_financial_evidence(
        list(eligibility_payload.get("entities") or []),
        source_payload,
    )
    validate_financial_evidence(entities)
    summary = financial_evidence_summary(entities)

    return {
        "artifact": "v2_entity_financial_evidence_inventory",
        "generated_at": _utc_now(),
        "status": "draft",
        "meta": {
            **dict(eligibility_payload.get("meta") or {}),
            **summary,
            "financial_reference_periods": dict(
                source_payload.get("reference_periods") or {}
            ),
            "financial_source": dict(source_payload.get("source") or {}),
            "financial_evidence_contract_note": (
                "This stage profiles evidence completeness only. It does not assign a score, "
                "rating, assessment eligibility, ranking eligibility or comparison cohort. "
                "A complete 12-month capital and balance history identifies an annual core "
                "observation window for methodology study; shorter histories remain visible "
                "as limited history rather than being treated as poor financial performance. "
                "Twenty-four and thirty-six month windows are retained for later stability "
                "and confidence tests."
            ),
        },
        "unresolved": eligibility_payload.get("unresolved") or {},
        "groups": eligibility_payload.get("groups") or [],
        "brands": eligibility_payload.get("brands") or [],
        "corporate_relationships": eligibility_payload.get(
            "corporate_relationships"
        )
        or [],
        "entities": entities,
    }


def write_financial_evidence_inventory(
    payload: dict[str, Any],
    output: Path = DEFAULT_OUTPUT,
) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(output.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(output)
    return output


def main() -> None:
    group_records = load_susep_economic_groups()
    ses_out = extract_ses_master_and_financials()
    if not isinstance(ses_out, tuple) or len(ses_out) < 2:
        raise RuntimeError(f"Unexpected SES return: {type(ses_out)}")

    classification = build_classification_inventory(
        ses_out[1],
        fetch_licensed_entities(),
        fetch_special_regime_records(),
        fetch_sandbox_participants(),
    )
    lifecycle = build_lifecycle_relationship_inventory(
        classification,
        load_lifecycle_records(),
        load_verified_relationship_registry(),
        group_records,
    )
    eligibility = build_eligibility_inventory(lifecycle)
    eligible_fips = [
        str(entity.get("fip_code") or "")
        for entity in eligibility["entities"]
        if (entity.get("eligibility") or {}).get("regulatory_universe_eligible")
        and entity.get("fip_code")
    ]
    source_payload = load_susep_financial_evidence(eligible_fips)
    payload = build_financial_evidence_inventory(eligibility, source_payload)
    path = write_financial_evidence_inventory(payload)
    meta = payload["meta"]
    print(
        "V2 financial evidence: "
        f"regulatory={meta['regulatory_eligible_count']} "
        f"core_ready={meta['core_financial_evidence_ready_count']} "
        f"states={meta['financial_evidence_state_counts']} "
        f"assessment={meta['assessment_eligible_count']} "
        f"ranking={meta['ranking_eligible_count']}; written to {path}"
    )


if __name__ == "__main__":
    main()
