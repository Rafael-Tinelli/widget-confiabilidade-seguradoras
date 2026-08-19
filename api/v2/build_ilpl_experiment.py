from __future__ import annotations

import json
import os
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from api.sources.susep_financial_evidence import (
    DEFAULT_SES_ZIP,
    load_susep_financial_evidence,
)
from api.v2.build_liquidity_experiment import load_validated_eligibility_artifact
from api.v2.financial_periods import apply_mature_financial_reference_period
from api.v2.ilpl_experiment import (
    ILPL_EQUITY_CMPID,
    ILPL_EXPERIMENT_VERSION,
    ILPL_NET_INCOME_CMPID,
    SURVIVAL_CRITERIA,
    SURVIVAL_CRITERIA_VERSION,
    build_entity_ilpl_experiment,
    evaluate_survival,
    ilpl_experiment_summary,
    validate_ilpl_experiment,
)

DEFAULT_OUTPUT = Path("data/derived/v2/ilpl_experiment.json")
ELIGIBILITY_INPUT_ENV = "V2_ELIGIBILITY_INPUT"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canon_fip_series(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip().str.replace(r"\.0+$", "", regex=True)
    digits = text.str.replace(r"\D+", "", regex=True)
    result = pd.Series([""] * len(digits), index=digits.index, dtype="object")
    valid = digits.str.len().fillna(0).astype(int) > 0
    result.loc[valid] = digits.loc[valid].str.zfill(6)
    return result


def _parse_number_series(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.strip()
    text = text.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})
    has_comma = text.str.contains(",", na=False)
    normalized = text.copy()
    normalized.loc[has_comma] = (
        normalized.loc[has_comma]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    normalized = normalized.str.replace(r"[^0-9.\-]", "", regex=True)
    return pd.to_numeric(normalized, errors="coerce")


def _find_balance_member(z: zipfile.ZipFile) -> str:
    match = next(
        (name for name in z.namelist() if name.lower().endswith("ses_balanco.csv")),
        None,
    )
    if not match:
        raise RuntimeError("SES_Balanco.csv not found in validated BaseCompleta.zip")
    return match


def read_ilpl_balance_history(
    fips: set[str], zip_path: Path = DEFAULT_SES_ZIP
) -> tuple[
    dict[str, dict[int, dict[int, float]]],
    dict[str, dict[int, int]],
]:
    """Read only CMPIDs 518 and 3333 for the closed ILPL experiment.

    Duplicate relevant rows are summed for auditability but the affected period
    is marked and later excluded from ILPL derivation. Nothing is imputed.
    """
    if not zip_path.exists() or not zipfile.is_zipfile(zip_path):
        raise RuntimeError(f"validated BaseCompleta.zip unavailable at {zip_path}")

    values: dict[str, dict[int, dict[int, float]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    duplicates: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    relevant_cmpids = {ILPL_NET_INCOME_CMPID, ILPL_EQUITY_CMPID}

    with zipfile.ZipFile(zip_path) as z:
        member = _find_balance_member(z)
        with z.open(member) as handle:
            header = list(
                pd.read_csv(handle, sep=";", encoding="latin1", nrows=0).columns
            )
        mapping = {column.lower().strip(): column for column in header}
        required = ["coenti", "damesano", "cmpid", "valor"]
        missing = [name for name in required if name not in mapping]
        if missing:
            raise RuntimeError(
                f"SES_Balanco.csv missing ILPL reader columns: {', '.join(missing)}"
            )
        usecols = [mapping[name] for name in required]

        with z.open(member) as handle:
            for chunk in pd.read_csv(
                handle,
                sep=";",
                encoding="latin1",
                dtype=str,
                usecols=usecols,
                chunksize=300_000,
                on_bad_lines="skip",
            ):
                chunk.columns = [column.lower().strip() for column in chunk.columns]
                chunk["fip"] = _canon_fip_series(chunk["coenti"])
                chunk = chunk[chunk["fip"].isin(fips)]
                if chunk.empty:
                    continue
                periods = pd.to_numeric(chunk["damesano"], errors="coerce")
                cmpids = pd.to_numeric(chunk["cmpid"], errors="coerce")
                amounts = _parse_number_series(chunk["valor"])

                for index, fip in chunk["fip"].items():
                    raw_period = periods.loc[index]
                    raw_cmpid = cmpids.loc[index]
                    amount = amounts.loc[index]
                    if pd.isna(raw_period) or pd.isna(raw_cmpid) or pd.isna(amount):
                        continue
                    period = int(raw_period)
                    cmpid = int(raw_cmpid)
                    if period <= 0 or cmpid not in relevant_cmpids:
                        continue
                    existing = values[fip][period].get(cmpid)
                    if existing is not None:
                        duplicates[fip][period] += 1
                        values[fip][period][cmpid] = existing + float(amount)
                    else:
                        values[fip][period][cmpid] = float(amount)

    return (
        {
            fip: {period: dict(by_cmpid) for period, by_cmpid in by_period.items()}
            for fip, by_period in values.items()
        },
        {fip: dict(by_period) for fip, by_period in duplicates.items()},
    )


def build_ilpl_experiment(
    eligibility_payload: dict[str, Any],
    source_payload: dict[str, Any],
    ilpl_values: dict[str, dict[int, dict[int, float]]],
    duplicate_rows: dict[str, dict[int, int]],
) -> dict[str, Any]:
    source_payload = apply_mature_financial_reference_period(source_payload)
    reference_period = (source_payload.get("reference_periods") or {}).get("balance")
    eligible_entities = [
        entity
        for entity in (eligibility_payload.get("entities") or [])
        if (entity.get("eligibility") or {}).get("regulatory_universe_eligible")
    ]
    entities = []
    for entity in eligible_entities:
        fip = str(entity.get("fip_code") or "").zfill(6)
        entities.append(
            build_entity_ilpl_experiment(
                entity,
                ilpl_values.get(fip, {}),
                duplicate_rows.get(fip, {}),
                reference_period,
            )
        )
    entities.sort(key=lambda item: str(item.get("entity_id") or ""))

    summary = ilpl_experiment_summary(entities, source_payload, reference_period)
    survival = evaluate_survival(summary)
    payload = {
        "artifact": "v2_ilpl_closed_experiment",
        "generated_at": _utc_now(),
        "status": "experimental_closed",
        "version": ILPL_EXPERIMENT_VERSION,
        "criteria_version": SURVIVAL_CRITERIA_VERSION,
        "criteria_locked_before_first_basecompleta_run": dict(SURVIVAL_CRITERIA),
        "source": dict(source_payload.get("source") or {}),
        "period_maturity": dict(source_payload.get("period_maturity") or {}),
        "summary": summary,
        "survival": survival,
        "entities": entities,
    }
    validate_ilpl_experiment(payload)
    return payload


def write_ilpl_experiment(payload: dict[str, Any], output: Path = DEFAULT_OUTPUT) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(output.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(output)
    return output


def main() -> None:
    eligibility_input = os.getenv(ELIGIBILITY_INPUT_ENV, "").strip()
    if not eligibility_input:
        raise RuntimeError(
            "Closed ILPL experiment requires a validated V2_ELIGIBILITY_INPUT artifact"
        )
    eligibility = load_validated_eligibility_artifact(Path(eligibility_input))
    eligible_entities = [
        entity
        for entity in eligibility["entities"]
        if (entity.get("eligibility") or {}).get("regulatory_universe_eligible")
        and entity.get("fip_code")
    ]
    eligible_fips = {str(entity["fip_code"]).zfill(6) for entity in eligible_entities}

    source_payload = load_susep_financial_evidence(sorted(eligible_fips))
    ilpl_values, duplicate_rows = read_ilpl_balance_history(eligible_fips)
    payload = build_ilpl_experiment(
        eligibility, source_payload, ilpl_values, duplicate_rows
    )
    path = write_ilpl_experiment(payload)
    summary = payload["summary"]
    survival = payload["survival"]
    print(
        "V2 ILPL closed experiment: "
        f"criteria={SURVIVAL_CRITERIA_VERSION} "
        f"entities={summary['entity_count']} "
        f"reference={summary['reference_period']} "
        f"current_n={summary['current_derivable_count']} "
        f"current_coverage={summary['current_coverage']} "
        f"paired_n={summary['paired_prior_equivalent_count']} "
        f"paired_coverage={summary['paired_prior_equivalent_coverage']} "
        f"same_month_stability={summary['same_month_rank_stability']['summary'].get('median_spearman')} "
        f"year_end_stability={summary['year_end_rank_stability']['summary'].get('median_spearman')} "
        f"sign_persistence={summary['sign_persistence'].get('rate')} "
        f"scale_spearman={summary['scale_bias_abs_ilpl_vs_average_equity'].get('spearman')} "
        f"pla_cmr_spearman={summary['redundancy']['pla_cmr'].get('spearman')} "
        f"ilt_spearman={summary['redundancy']['ilt'].get('spearman')} "
        f"survives={survival['survives_as_independent_scoring_candidate']} "
        f"failed={survival['failed_gates']}; written to {path}"
    )


if __name__ == "__main__":
    main()
