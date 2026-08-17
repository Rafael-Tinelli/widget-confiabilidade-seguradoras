from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.sources.receita_cnpj import load_lifecycle_records
from api.sources.ses import extract_ses_master_and_financials
from api.sources.susep_groups import load_susep_economic_groups
from api.sources.susep_licensed import fetch_licensed_entities
from api.sources.susep_sandbox import fetch_sandbox_participants
from api.sources.susep_special_regimes import fetch_special_regime_records
from api.v2.build_classification_inventory import build_classification_inventory
from api.v2.build_lifecycle_relationship_inventory import (
    build_lifecycle_relationship_inventory,
)
from api.v2.eligibility import (
    apply_eligibility,
    eligibility_summary,
    validate_eligibility,
)
from api.v2.relationships import load_verified_relationship_registry

DEFAULT_OUTPUT = Path("data/derived/v2/entity_eligibility_inventory.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_eligibility_inventory(
    lifecycle_payload: dict[str, Any],
) -> dict[str, Any]:
    entities = apply_eligibility(list(lifecycle_payload.get("entities") or []))
    validate_eligibility(entities)
    summary = eligibility_summary(entities)

    return {
        "artifact": "v2_entity_eligibility_inventory",
        "generated_at": _utc_now(),
        "status": "draft",
        "meta": {
            **dict(lifecycle_payload.get("meta") or {}),
            **summary,
            "eligibility_contract_note": (
                "The regulatory universe contains only current ordinary insurers licensed "
                "by SUSEP. Receita CNPJ lifecycle is an independent legal cross-check and "
                "does not itself grant or revoke a SUSEP license. Sandbox, special regimes, "
                "historical entities, pension, capitalization, reinsurance and other entity "
                "types remain searchable but outside the ordinary-insurer comparison universe. "
                "Passing the regulatory gate does not make an entity assessment-eligible or "
                "ranking-eligible: financial evidence, complaints evidence, methodology "
                "calibration, confidence and comparison-cohort gates remain pending."
            ),
        },
        "unresolved": lifecycle_payload.get("unresolved") or {},
        "groups": lifecycle_payload.get("groups") or [],
        "brands": lifecycle_payload.get("brands") or [],
        "corporate_relationships": lifecycle_payload.get("corporate_relationships") or [],
        "entities": entities,
    }


def write_eligibility_inventory(
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
    payload = build_eligibility_inventory(lifecycle)
    path = write_eligibility_inventory(payload)
    meta = payload["meta"]
    print(
        "V2 eligibility: "
        f"regulatory={meta['regulatory_universe_eligible_count']} "
        f"assessment={meta['assessment_eligible_count']} "
        f"ranking={meta['ranking_eligible_count']} "
        f"entities={len(payload['entities'])}; written to {path}"
    )


if __name__ == "__main__":
    main()
