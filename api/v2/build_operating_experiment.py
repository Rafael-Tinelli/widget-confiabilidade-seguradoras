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
from api.v2.build_liquidity_experiment import load_validated_eligibility_artifact
from api.v2.financial_periods import apply_mature_financial_reference_period
from api.v2.operating_experiment import (
    build_entity_operating_experiment,
    operating_experiment_summary,
    validate_operating_experiment,
)
from api.v2.operating_states import build_operating_state, operating_state_summary
from api.v2.relationships import load_verified_relationship_registry

DEFAULT_OUTPUT = Path("data/derived/v2/operating_experiment.json")
ELIGIBILITY_INPUT_ENV = "V2_ELIGIBILITY_INPUT"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_operating_experiment(
    eligibility_payload: dict[str, Any],
    source_payload: dict[str, Any],
) -> dict[str, Any]:
    source_payload = apply_mature_financial_reference_period(source_payload)
    reference_period = (source_payload.get("reference_periods") or {}).get("balance")
    source_entities = source_payload.get("entities") or {}
    eligible_entities = [
        entity
        for entity in (eligibility_payload.get("entities") or [])
        if (entity.get("eligibility") or {}).get("regulatory_universe_eligible")
    ]
    entities = [
        build_entity_operating_experiment(
            entity,
            source_entities.get(str(entity.get("fip_code") or "").zfill(6), {}),
            reference_period,
        )
        for entity in eligible_entities
    ]
    for entity in entities:
        entity["operating_state"] = build_operating_state(entity, reference_period)
    entities.sort(key=lambda item: str(item.get("entity_id") or ""))

    summary = operating_experiment_summary(
        entities,
        source_payload,
        reference_period,
    )
    summary["operating_states"] = operating_state_summary(entities)
    payload = {
        "artifact": "v2_operating_experiment",
        "generated_at": _utc_now(),
        "status": "experimental",
        "source": dict(source_payload.get("source") or {}),
        "period_maturity": dict(source_payload.get("period_maturity") or {}),
        "summary": summary,
        "entities": entities,
    }
    validate_operating_experiment(payload)
    return payload


def write_operating_experiment(
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
    payload = build_operating_experiment(eligibility, source_payload)
    path = write_operating_experiment(payload)
    summary = payload["summary"]
    ic = summary["metrics"]["IC"]
    ica = summary["metrics"]["ICA"]
    states = summary["operating_states"]
    print(
        "V2 operating experiment: "
        f"eligibility_source={eligibility_source} "
        f"entities={summary['entity_count']} "
        f"reference={summary['reference_period']} "
        f"maturity={payload['period_maturity'].get('status')} "
        f"IC_n={ic['current_distribution'].get('count', 0)} "
        f"IC_median={ic['current_distribution'].get('median')} "
        f"ICA_n={ica['current_distribution'].get('count', 0)} "
        f"ICA_median={ica['current_distribution'].get('median')} "
        f"IC_ICA_spearman={summary['current_ic_ica_correlation'].get('spearman')} "
        f"IC_ILT_spearman={ic['ilt_correlation'].get('spearman')} "
        f"IC_capital_spearman={ic['capital_pla_cmr_correlation'].get('spearman')} "
        f"operating_signals={states.get('operating_signal_counts')}; "
        f"written to {path}"
    )


if __name__ == "__main__":
    main()
