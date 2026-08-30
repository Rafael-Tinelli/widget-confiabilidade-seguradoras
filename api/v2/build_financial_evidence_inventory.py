from __future__ import annotations

import argparse
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
from api.v2.financial_periods import apply_mature_financial_reference_period
from api.v2.relationships import load_verified_relationship_registry

DEFAULT_OUTPUT = Path("data/derived/v2/entity_financial_evidence_inventory.json")
DEFAULT_ELIGIBILITY_INPUT = Path("data/derived/v2/entity_eligibility_inventory.json")
DEFAULT_SES_ZIP = Path("data/raw/ses/BaseCompleta.zip")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_financial_evidence_inventory(
    eligibility_payload: dict[str, Any],
    source_payload: dict[str, Any],
) -> dict[str, Any]:
    source_payload = apply_mature_financial_reference_period(source_payload)
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
            "financial_period_maturity": dict(
                source_payload.get("period_maturity") or {}
            ),
            "financial_source": dict(source_payload.get("source") or {}),
            "financial_evidence_contract_note": (
                "This stage profiles evidence completeness only. It does not assign a score, "
                "rating, assessment eligibility, ranking eligibility or comparison cohort. "
                "Financial evidence is aligned to the latest common mature period rather than "
                "blindly using the latest observed month when prudential coverage is still "
                "materially incomplete. A complete 12-month capital and balance history "
                "identifies an annual core observation window for methodology study; shorter "
                "histories remain visible as limited history rather than being treated as poor "
                "financial performance. Twenty-four and thirty-six month windows are retained "
                "for later stability and confidence tests."
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


def _build_legacy_eligibility_from_sources() -> dict[str, Any]:
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
    return build_eligibility_inventory(lifecycle)


def _load_eligibility_input(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("artifact") != "v2_entity_eligibility_inventory":
        raise RuntimeError("unexpected eligibility input artifact")
    return payload


def _eligible_fips(eligibility: dict[str, Any]) -> list[str]:
    return [
        str(entity.get("fip_code") or "")
        for entity in eligibility.get("entities") or []
        if (entity.get("eligibility") or {}).get("regulatory_universe_eligible")
        and entity.get("fip_code")
    ]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the v2 financial evidence inventory.")
    parser.add_argument(
        "--eligibility-input",
        type=Path,
        help=(
            "Use an already materialized eligibility inventory. Gate 4 uses this mode "
            "so this builder does not refetch regulatory sources."
        ),
    )
    parser.add_argument(
        "--ses-zip",
        type=Path,
        default=DEFAULT_SES_ZIP,
        help="Validated BaseCompleta.zip snapshot used for financial derivation.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.eligibility_input is not None:
        eligibility = _load_eligibility_input(args.eligibility_input)
        financial_zip: Path | None = args.ses_zip
    else:
        eligibility = _build_legacy_eligibility_from_sources()
        financial_zip = None

    source_payload = load_susep_financial_evidence(
        _eligible_fips(eligibility),
        zip_path=financial_zip,
    )
    payload = build_financial_evidence_inventory(eligibility, source_payload)
    path = write_financial_evidence_inventory(payload, args.output)
    meta = payload["meta"]
    maturity = meta.get("financial_period_maturity") or {}
    print(
        "V2 financial evidence: "
        f"regulatory={meta['regulatory_eligible_count']} "
        f"reference={meta['financial_reference_periods'].get('capital')} "
        f"maturity={maturity.get('status')} "
        f"core_ready={meta['core_financial_evidence_ready_count']} "
        f"states={meta['financial_evidence_state_counts']} "
        f"assessment={meta['assessment_eligible_count']} "
        f"ranking={meta['ranking_eligible_count']}; written to {path}"
    )


if __name__ == "__main__":
    main()
