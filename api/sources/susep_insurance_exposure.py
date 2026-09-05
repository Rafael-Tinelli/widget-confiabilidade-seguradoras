from __future__ import annotations

import math
import os
import zipfile
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_SES_ZIP = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")) / "BaseCompleta.zip"
SES_TABLE_DOCUMENTATION_URL = (
    "https://www2.susep.gov.br/menuestatistica/SES/download/Documentacao_das_tabelas.rtf"
)
REQUIRED_COLUMNS = {
    "damesano",
    "coenti",
    "coramo",
    "premio_direto",
    "premio_ganho",
}


class InsuranceExposureSourceError(RuntimeError):
    """Raised when SES insurance production cannot be read safely."""


def _canon_fip(value: Any) -> str:
    text = str(value or "").strip().removesuffix(".0")
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _premium_number(value: Any, *, field: str) -> float | None:
    """Parse one SES premium cell without conflating missing and malformed values."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        number = float(text)
    except ValueError as exc:
        raise InsuranceExposureSourceError(
            f"invalid {field} value in Ses_seguros.csv: {value!r}"
        ) from exc
    if not math.isfinite(number):
        raise InsuranceExposureSourceError(
            f"non-finite {field} value in Ses_seguros.csv: {value!r}"
        )
    return number


def _missing_cell(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return str(value).strip().lower() in {"", "nan"}


def _integer(value: Any, *, field: str) -> int:
    text = str(value or "").strip()
    try:
        number = float(text)
    except ValueError as exc:
        raise InsuranceExposureSourceError(
            f"invalid {field} value in Ses_seguros.csv: {value!r}"
        ) from exc
    if not math.isfinite(number) or not number.is_integer():
        raise InsuranceExposureSourceError(
            f"invalid {field} value in Ses_seguros.csv: {value!r}"
        )
    return int(number)


def _period(value: Any) -> int:
    period = _integer(value, field="damesano")
    year = period // 100
    month = period % 100
    if not 1000 <= year <= 9999 or not 1 <= month <= 12:
        raise InsuranceExposureSourceError(
            f"invalid damesano value in Ses_seguros.csv: {value!r}"
        )
    return period


def _insurance_member(z: zipfile.ZipFile) -> str:
    member = next(
        (name for name in z.namelist() if name.lower().endswith("ses_seguros.csv")),
        None,
    )
    if not member:
        raise InsuranceExposureSourceError("Ses_seguros.csv not found in BaseCompleta.zip")
    return member


def probe_susep_insurance_exposure(zip_path: Path | None = None) -> dict[str, Any]:
    """Validate only the insurance-production component needed for Conduct pressure."""
    path = Path(zip_path or DEFAULT_SES_ZIP)
    if not path.exists():
        return {"state": "source_unavailable", "reason": "ses_basecompleta_missing"}
    if not zipfile.is_zipfile(path):
        return {"state": "source_invalid", "reason": "ses_basecompleta_not_zip"}
    try:
        with zipfile.ZipFile(path) as z:
            member = _insurance_member(z)
            with z.open(member) as handle:
                frame = pd.read_csv(handle, sep=";", encoding="latin1", nrows=0)
            header = {str(column).strip().lower() for column in frame.columns}
    except (OSError, ValueError, zipfile.BadZipFile, InsuranceExposureSourceError) as exc:
        return {
            "state": "source_invalid",
            "reason": "ses_insurance_exposure_unreadable",
            "error": str(exc),
        }

    missing = sorted(REQUIRED_COLUMNS - header)
    return {
        "state": "available" if not missing else "schema_incompatible",
        "member": member,
        "required_columns": sorted(REQUIRED_COLUMNS),
        "missing_columns": missing,
        "exposure_domain": "insurance_only",
        "primary_candidate": "insurance_premium_direct",
        "diagnostic_only": "insurance_premium_earned",
        "currency": "BRL",
        "source_unit_label": "R$",
        "scale_factor_applied": 1.0,
        "source_documentation_url": SES_TABLE_DOCUMENTATION_URL,
        "explicitly_excluded_domains": ["private_pension", "capitalization"],
    }


def load_susep_insurance_exposure(
    fip_codes: Iterable[str],
    zip_path: Path | None = None,
) -> dict[str, Any]:
    """Load insurance premiums only; never reads pension or capitalization files.

    Missing cells in the approved direct-premium denominator are preserved as
    missingness metadata. They are never silently converted to economic zero.
    Malformed numeric cells and malformed period/branch keys fail closed. A row
    without a branch is ignored only when both premium fields are explicit zero;
    this preserves the two known empty historical SES rows without inventing a
    branch or changing an exposure denominator.
    Values remain in the source SES unit (R$); no scale conversion is applied.
    """
    fips = {_canon_fip(value) for value in fip_codes}
    fips.discard("")
    if not fips:
        raise InsuranceExposureSourceError("at least one FIP code is required")

    path = Path(zip_path or DEFAULT_SES_ZIP)
    probe = probe_susep_insurance_exposure(path)
    if probe.get("state") != "available":
        raise InsuranceExposureSourceError(f"SES insurance exposure unavailable: {probe}")

    aggregate: dict[tuple[str, int, int], dict[str, float]] = defaultdict(
        lambda: {
            "premium_direct": 0.0,
            "premium_earned": 0.0,
            "rows": 0.0,
            "premium_direct_missing_rows": 0.0,
            "premium_earned_missing_rows": 0.0,
        }
    )
    periods: set[int] = set()
    ignored_unclassified_zero_premium_rows = 0

    with zipfile.ZipFile(path) as z, z.open(str(probe["member"])) as handle:
        for chunk in pd.read_csv(
            handle,
            sep=";",
            encoding="latin1",
            dtype=str,
            usecols=lambda column: str(column).strip().lower() in REQUIRED_COLUMNS,
            chunksize=300_000,
            on_bad_lines="error",
        ):
            chunk.columns = [str(column).strip().lower() for column in chunk.columns]
            for row in chunk.to_dict(orient="records"):
                fip = _canon_fip(row.get("coenti"))
                if fip not in fips:
                    continue
                period = _period(row.get("damesano"))
                direct = _premium_number(row.get("premio_direto"), field="premio_direto")
                earned = _premium_number(row.get("premio_ganho"), field="premio_ganho")
                branch_raw = row.get("coramo")
                if _missing_cell(branch_raw):
                    if direct == 0.0 and earned == 0.0:
                        ignored_unclassified_zero_premium_rows += 1
                        continue
                    raise InsuranceExposureSourceError(
                        "missing coramo value in Ses_seguros.csv is only allowed "
                        "when premio_direto and premio_ganho are explicit zero"
                    )
                branch = _integer(branch_raw, field="coramo")

                key = (fip, period, branch)
                values = aggregate[key]
                values["rows"] += 1.0
                if direct is None:
                    values["premium_direct_missing_rows"] += 1.0
                else:
                    values["premium_direct"] += direct
                if earned is None:
                    values["premium_earned_missing_rows"] += 1.0
                else:
                    values["premium_earned"] += earned
                periods.add(period)

    entities: dict[str, Any] = {fip: {"months": {}} for fip in sorted(fips)}
    for (fip, period, branch), values in sorted(aggregate.items()):
        month = entities[fip]["months"].setdefault(
            period,
            {
                "insurance_premium_direct": 0.0,
                "insurance_premium_earned": 0.0,
                "insurance_premium_direct_missing_rows": 0,
                "insurance_premium_earned_missing_rows": 0,
                "insurance_branches": {},
            },
        )
        month["insurance_premium_direct"] += values["premium_direct"]
        month["insurance_premium_earned"] += values["premium_earned"]
        month["insurance_premium_direct_missing_rows"] += int(
            values["premium_direct_missing_rows"]
        )
        month["insurance_premium_earned_missing_rows"] += int(
            values["premium_earned_missing_rows"]
        )
        month["insurance_branches"][branch] = dict(values)

    return {
        "source": {
            "source_id": "susep_ses_insurance_exposure",
            "authority": "SUSEP",
            "source_file": "BaseCompleta.zip",
            "component_file": "Ses_seguros.csv",
            "exposure_domain": "insurance_only",
            "primary_candidate": "insurance_premium_direct",
            "diagnostic_only": "insurance_premium_earned",
            "currency": "BRL",
            "source_unit_label": "R$",
            "scale_factor_applied": 1.0,
            "unit_policy": "raw_ses_currency_values_no_scale_conversion",
            "source_documentation_url": SES_TABLE_DOCUMENTATION_URL,
            "missingness_policy": "missing_premium_cells_are_not_economic_zero",
            "malformed_value_policy": "fail_closed_not_missing",
            "malformed_row_policy": "fail_closed_not_skipped",
            "unclassified_branch_policy": (
                "ignore_only_when_direct_and_earned_premiums_are_explicit_zero;"
                "otherwise_fail_closed"
            ),
            "ignored_unclassified_zero_premium_rows": (
                ignored_unclassified_zero_premium_rows
            ),
            "explicitly_excluded_domains": ["private_pension", "capitalization"],
            "scoring": "forbidden_in_source_artifact",
        },
        "periods": sorted(periods),
        "reference_period": max(periods) if periods else None,
        "entities": entities,
    }
