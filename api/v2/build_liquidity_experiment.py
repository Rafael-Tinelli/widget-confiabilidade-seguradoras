from __future__ import annotations

import json
import os
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
from api.v2.eligibility import validate_eligibility
from api.v2.financial_periods import apply_mature_financial_reference_period
from api.v2.liquidity_diagnostics import build_liquidity_diagnostics
from api.v2.liquidity_experiment import (
    build_entity_liquidity_experiment,
    liquidity_experiment_summary,
    validate_liquidity_experiment,
)
from api.v2.relationships import load_verified_relationship_registry

DEFAULT_OUTPUT = Path("data/derived/v2/liquidity_experiment.json")
ELIGIBILITY_INPUT_ENV = "V2_ELIGIBILITY_INPUT"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_validated_eligibility_artifact(path: Path) -> dict[str, Any]:
    """Load a previously validated upstream eligibility snapshot.

    Downstream financial experiments should not need to re-query live regulatory
    identity sources when their input contract is already materialized by the
    eligibility workflow. The artifact is still revalidated before use.
    """
    if not path.exists():
        raise RuntimeError(f"Eligibility artifact unavailable at {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("artifact") != "v2_entity_eligibility_inventory":
        raise RuntimeError("Unexpected eligibility artifact contract")
    entities = list(payload.get("entities") or [])
    validate_eligibility(entities)
    expected = int(
        ((payload.get("meta") or {}).get("regulatory_universe_eligible_count")) or 0
    )
    actual = sum(
        bool((entity.get("eligibility") or {}).get("regulatory_universe_eligible"))
        for entity in entities
    )
    if expected != actual:
        raise RuntimeError(
            f"Eligibility artifact count mismatch: meta={expected} actual={actual}"
        )
    if actual <= 0:
        raise RuntimeError("Eligibility artifact contains no regulatory universe")
    return payload


def build_liquidity_experiment(
    eligibility_payload: dict[str, Any],
    source_payload: dict[str, Any],
) -> dict[str, Any]:
    source_payload = apply_mature_financial_reference_period(source_payload)
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
    summary["period_maturity"] = dict(source_payload.get("period_maturity") or {})
    summary["diagnostics"] = build_liquidity_diagnostics(entities, source_payload)
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


def _build_live_eligibility() -> dict[str, Any]:
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


def main() -> None:
    eligibility_input = os.getenv(ELIGIBILITY_INPUT_ENV, "").strip()
    if eligibility_input:
        eligibility = load_validated_eligibility_artifact(Path(eligibility_input))
        eligibility_source = f"validated_artifact:{eligibility_input}"
    else:
        eligibility = _build_live_eligibility()
        eligibility_source = "live_upstream_rebuild"

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
    diagnostics = summary["diagnostics"]["metrics"]
    maturity = summary.get("period_maturity") or {}
    print(
        "V2 liquidity experiment: "
        f"eligibility_source={eligibility_source} "
        f"entities={summary['entity_count']} "
        f"reference={summary.get('reference_period')} "
        f"maturity={maturity.get('status')} "
        f"quality_excluded={summary['quality_excluded_count']} "
        f"ILC_n={ilc.get('count', 0)} ILC_median={ilc.get('median')} "
        f"ILT_n={ilt.get('count', 0)} ILT_median={ilt.get('median')} "
        f"paired={corr.get('count')} spearman={corr.get('spearman')} "
        f"ILC_capital_spearman={diagnostics['ILC']['capital_pla_cmr_redundancy']['raw']['spearman']} "
        f"ILT_capital_spearman={diagnostics['ILT']['capital_pla_cmr_redundancy']['raw']['spearman']}; "
        f"written to {path}"
    )


if __name__ == "__main__":
    main()
