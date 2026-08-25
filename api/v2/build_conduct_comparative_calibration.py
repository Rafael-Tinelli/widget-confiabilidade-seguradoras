from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from api.sources.susep_conduct_exposure import (
    DEFAULT_SES_ZIP,
    load_susep_conduct_exposure,
)
from api.v2.conduct_comparative import (
    exposure_comparability_state,
    expected_complaints,
    pressure_ratio,
)

CONDUCT_EVIDENCE_PATH = Path("data/derived/v2/consumer_gov_conduct_evidence.json")
OUTPUT_PATH = Path("data/derived/v2/conduct_comparative_calibration.json")
CALIBRATION_VERSION = "2.0-draft-conduct-comparative-calibration-1"
CANDIDATE_DENOMINATOR = "insurance_premium_direct_plus_private_pension_contributions"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _period(value: Any) -> int:
    text = str(value or "").strip().replace("-", "")
    if len(text) != 6 or not text.isdigit():
        raise ValueError(f"invalid monthly period: {value!r}")
    year = int(text[:4])
    month = int(text[4:])
    if year < 2000 or not 1 <= month <= 12:
        raise ValueError(f"invalid monthly period: {value!r}")
    return int(text)


def _finite(value: Any) -> float:
    number = float(value or 0.0)
    if not math.isfinite(number):
        raise ValueError(f"non-finite exposure component: {value!r}")
    return number


def _sum_entity_exposure(
    ses_entity: dict[str, Any],
    periods: list[int],
) -> dict[str, Any]:
    direct = 0.0
    private_pension = 0.0
    insurance_rows = 0
    private_pension_rows = 0
    insurance_months_with_rows = 0
    private_pension_months_with_rows = 0
    branches: Counter[str] = Counter()
    pension_products: Counter[str] = Counter()

    months = ses_entity.get("months") or {}
    for period in periods:
        month = months.get(period) or {}
        direct += _finite(month.get("insurance_premium_direct"))
        private_pension += _finite(month.get("pension_contributions"))

        month_insurance_rows = 0
        for branch, values in (month.get("insurance_branches") or {}).items():
            row_count = int(float((values or {}).get("rows") or 0))
            amount = _finite((values or {}).get("premium_direct"))
            month_insurance_rows += row_count
            if amount != 0.0:
                branches[str(branch)] += amount
        insurance_rows += month_insurance_rows
        insurance_months_with_rows += int(month_insurance_rows > 0)

        month_pension_rows = 0
        for product, values in (month.get("pension_products") or {}).items():
            row_count = int(float((values or {}).get("rows") or 0))
            amount = _finite((values or {}).get("contributions"))
            month_pension_rows += row_count
            if amount != 0.0:
                pension_products[str(product)] += amount
        private_pension_rows += month_pension_rows
        private_pension_months_with_rows += int(month_pension_rows > 0)

    combined = direct + private_pension
    return {
        "insurance_premium_direct": float(direct),
        "private_pension_contributions": float(private_pension),
        "combined_revenue_candidate": float(combined),
        "insurance_rows": insurance_rows,
        "private_pension_rows": private_pension_rows,
        "insurance_months_with_rows": insurance_months_with_rows,
        "private_pension_months_with_rows": private_pension_months_with_rows,
        "insurance_branch_premium_direct": {
            key: float(value) for key, value in sorted(branches.items())
        },
        "private_pension_product_contributions": {
            key: float(value) for key, value in sorted(pension_products.items())
        },
    }


def _comparability(
    complaints: float,
    exposure: dict[str, Any],
    *,
    fip_code: str | None,
) -> dict[str, Any]:
    if not fip_code:
        return {
            "state": "exposure_unavailable",
            "pressure_eligible": False,
            "reason_code": "missing_fip_code",
        }

    direct = float(exposure["insurance_premium_direct"])
    pension = float(exposure["private_pension_contributions"])
    if direct < 0 or pension < 0:
        return {
            "state": "negative_exposure_component",
            "pressure_eligible": False,
            "reason_code": "negative_revenue_component_requires_investigation",
        }
    return exposure_comparability_state(
        complaints,
        float(exposure["combined_revenue_candidate"]),
    )


def _quantiles(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"p10": None, "p25": None, "p50": None, "p75": None, "p90": None}
    return {
        "p10": float(np.percentile(values, 10)),
        "p25": float(np.percentile(values, 25)),
        "p50": float(np.percentile(values, 50)),
        "p75": float(np.percentile(values, 75)),
        "p90": float(np.percentile(values, 90)),
    }


def build_calibration(
    conduct: dict[str, Any],
    ses: dict[str, Any],
) -> dict[str, Any]:
    conduct_months = [
        str(month) for month in (conduct.get("source") or {}).get("months") or []
    ]
    if not conduct_months:
        raise RuntimeError("Consumer.gov conduct artifact has no months")
    periods = [_period(month) for month in conduct_months]
    if len(periods) != len(set(periods)):
        raise RuntimeError("Consumer.gov conduct months contain duplicates")

    ses_periods = {int(period) for period in ses.get("periods") or []}
    missing_periods = [period for period in periods if period not in ses_periods]
    if missing_periods:
        raise RuntimeError(
            "SES exposure does not cover the complete Consumer.gov comparison window: "
            f"{missing_periods}"
        )

    conduct_entities = list(conduct.get("entities") or [])
    if not conduct_entities:
        raise RuntimeError("Consumer.gov conduct artifact contains no entities")

    ses_entities = ses.get("entities") or {}
    rows: list[dict[str, Any]] = []
    states: Counter[str] = Counter()

    for entity in conduct_entities:
        entity_id = str(entity.get("entity_id") or "")
        fip_raw = entity.get("fip_code")
        fip = str(fip_raw or "").strip()
        if fip:
            digits = "".join(ch for ch in fip if ch.isdigit())
            fip = digits.zfill(6) if digits else ""

        complaints = float((entity.get("totals") or {}).get("complaints") or 0.0)
        exposure = _sum_entity_exposure(
            (ses_entities.get(fip) or {"months": {}}),
            periods,
        )
        comparability = _comparability(
            complaints,
            exposure,
            fip_code=fip or None,
        )
        states[str(comparability["state"])] += 1

        rows.append(
            {
                "entity_id": entity_id,
                "fip_code": fip or None,
                "cnpj": entity.get("cnpj"),
                "legal_name": entity.get("legal_name"),
                "display_name": entity.get("display_name"),
                "complaints_12m": int(complaints),
                "revenue_components_12m": exposure,
                "comparability": comparability,
                "pressure": {
                    "state": "pending_market_baseline"
                    if comparability["pressure_eligible"]
                    else "not_comparable",
                    "expected_complaints": None,
                    "pressure_ratio": None,
                    "complaint_share": None,
                    "revenue_share": None,
                    "complaints_per_billion_combined_revenue": None,
                },
            }
        )

    comparable = [
        row for row in rows if (row.get("comparability") or {}).get("pressure_eligible")
    ]
    market_complaints = float(sum(row["complaints_12m"] for row in comparable))
    market_revenue = float(
        sum(
            row["revenue_components_12m"]["combined_revenue_candidate"]
            for row in comparable
        )
    )
    if not comparable or market_revenue <= 0:
        raise RuntimeError("no comparable population available for conduct calibration")

    pressure_values: list[float] = []
    for row in rows:
        if not row["comparability"]["pressure_eligible"]:
            continue
        revenue = float(row["revenue_components_12m"]["combined_revenue_candidate"])
        complaints = float(row["complaints_12m"])
        expected = expected_complaints(revenue, market_complaints, market_revenue)
        ratio = pressure_ratio(
            complaints,
            revenue,
            market_complaints,
            market_revenue,
        )
        if expected is None or ratio is None:
            raise RuntimeError(f"comparable entity produced no pressure: {row['entity_id']}")
        row["pressure"] = {
            "state": "available",
            "expected_complaints": float(expected),
            "pressure_ratio": float(ratio),
            "complaint_share": (
                float(complaints / market_complaints) if market_complaints > 0 else None
            ),
            "revenue_share": float(revenue / market_revenue),
            "complaints_per_billion_combined_revenue": float(
                complaints / revenue * 1_000_000_000
            ),
        }
        pressure_values.append(float(ratio))

    return {
        "artifact": "v2_conduct_comparative_calibration",
        "generated_at": _utc_now(),
        "status": "experimental",
        "version": CALIBRATION_VERSION,
        "assessment_role": "comparative_conduct_calibration_only",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "candidate_denominator": {
            "id": CANDIDATE_DENOMINATOR,
            "selected_for_scoring": False,
            "formula": "insurance_premium_direct + private_pension_contributions",
            "semantics": {
                "insurance_premium_direct": (
                    "Direct insurance premiums reported in Ses_seguros.csv."
                ),
                "private_pension_contributions": (
                    "Private-pension contributions reported in Ses_Contrib_Benef.csv. "
                    "PGBL, VGBL and PrevTrad are treated as private-pension products, "
                    "not as insurance premiums."
                ),
                "combined_revenue_candidate": (
                    "Experimental combined entity revenue denominator used only because "
                    "the Consumer.gov provider-level complaint numerator can mix insurance "
                    "and private-pension activity. Components remain separately auditable."
                ),
            },
        },
        "source": {
            "consumer_gov_conduct_artifact": str(CONDUCT_EVIDENCE_PATH),
            "ses_source_artifact": str(DEFAULT_SES_ZIP),
            "months": conduct_months,
            "periods": periods,
            "population_policy": "complaints_and_revenue_same_entities_only",
        },
        "market_baseline": {
            "state": "available",
            "comparable_entities": len(comparable),
            "excluded_entities": len(rows) - len(comparable),
            "excluded_by_state": {
                key: int(value)
                for key, value in sorted(states.items())
                if key != "comparable"
            },
            "market_complaints": int(market_complaints),
            "market_combined_revenue_candidate": float(market_revenue),
            "population_policy": "complaints_and_revenue_same_entities_only",
        },
        "diagnostics": {
            "comparability_state_counts": dict(sorted(states.items())),
            "pressure_ratio_quantiles": _quantiles(pressure_values),
            "pressure_ratio_available_entities": len(pressure_values),
            "candidate_denominator_not_final": True,
            "cohorts_selected": False,
            "shrinkage_applied": False,
            "taxonomy_required": False,
        },
        "entities": sorted(
            rows,
            key=lambda row: (
                str(row.get("legal_name") or row.get("display_name") or ""),
                str(row.get("entity_id") or ""),
            ),
        ),
    }


def build_from_files(
    *,
    conduct_path: Path = CONDUCT_EVIDENCE_PATH,
    ses_path: Path = DEFAULT_SES_ZIP,
) -> dict[str, Any]:
    conduct = json.loads(conduct_path.read_text(encoding="utf-8"))
    fips = [
        str(entity.get("fip_code") or "")
        for entity in conduct.get("entities") or []
        if str(entity.get("fip_code") or "").strip()
    ]
    if not fips:
        raise RuntimeError("Consumer.gov conduct artifact contains no FIP codes")
    ses = load_susep_conduct_exposure(fips, ses_path)
    return build_calibration(conduct, ses)


def main() -> None:
    payload = build_from_files()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "version": payload["version"],
                "months": payload["source"]["months"],
                "market_baseline": payload["market_baseline"],
                "diagnostics": payload["diagnostics"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
