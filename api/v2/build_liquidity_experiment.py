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
from api.v2.liquidity_experiment import (
    build_entity_liquidity_experiment,
    liquidity_experiment_summary,
    validate_liquidity_experiment,
)
from api.v2.relationships import load_verified_relationship_registry

DEFAULT_OUTPUT = Path("data/derived/v2/liquidity_experiment.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_liquidity_experiment(
    eligibility_payload: dict[str, Any],
    source_payload: dict[str, Any],
) -> dict[str, Any]:
    source_entities = source_payload.get("entities") or {}
    reference_period = (source_payload.get("reference_periods") or {}).get("balance")
    eligible_entities = [
        entity
        for entity in (eligibility_payload.get("entities") or [])
        if (entity.get("eligibility") or {}).get("regulatory_universe_eligible")
    ]
    entities = [
        build_entity_liquidity_experiment(
            entity,
            source_entities.get(str(entity.get("fip_code") or "").zfill(6), {}),
            reference_period,
        )
        for entity in eligible_entities
    ]
    entities.sort(key=lambda item: str(item.get("entity_id") or ""))
    summary = liquidity_experiment_summary(entities, reference_period)
    payload = {
        "artifact": "v2_liquidity_experiment",
        "generated_at": _utc_now(),
        "status": "experimental",
        "source": dict(source_payload.get("source") or {}),
        "summary": summary,
        "entities": entities,
    }
    validate_liquidity_experiment(payload)
    return payload


def write_liquidity_experiment(
    payload: dict[str, Any], output: Path = DEFAULT_OUTPUT
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
    payload = build_liquidity_experiment(eligibility, source_payload)
    path = write_liquidity_experiment(payload)
    summary = payload["summary"]
    ilc = summary["metrics"]["ILC"]["current_distribution_excluding_quality_issues"]
    ilt = summary["metrics"]["ILT"]["current_distribution_excluding_quality_issues"]
    corr = summary["current_ilc_ilt_correlation"]
    print(
        "V2 liquidity experiment: "
        f"entities={summary['entity_count']} "
        f"quality_excluded={summary['quality_excluded_count']} "
        f"ILC_n={ilc.get('count', 0)} ILC_median={ilc.get('median')} "
        f"ILT_n={ilt.get('count', 0)} ILT_median={ilt.get('median')} "
        f"paired={corr.get('count')} spearman={corr.get('spearman')}; "
        f"written to {path}"
    )


if __name__ == "__main__":
    main()
