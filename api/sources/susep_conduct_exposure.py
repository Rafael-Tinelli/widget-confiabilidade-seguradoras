from __future__ import annotations

import os
import zipfile
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_SES_ZIP = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")) / "BaseCompleta.zip"
INSURANCE_REQUIRED_COLUMNS = {
    "damesano",
    "coenti",
    "coramo",
    "premio_direto",
    "premio_ganho",
}
PENSION_REQUIRED_COLUMNS = {"damesano", "coenti", "tipoprod", "contrib"}


class ConductExposureSourceError(RuntimeError):
    """Raised when the SES source cannot support conduct exposure calibration."""


def _canon_fip(value: Any) -> str:
    text = str(value or "").strip().removesuffix(".0")
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _parse_number(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _member(z: zipfile.ZipFile, basename: str) -> str:
    match = next(
        (name for name in z.namelist() if name.lower().endswith(basename.lower())),
        None,
    )
    if not match:
        raise ConductExposureSourceError(f"{basename} not found in BaseCompleta.zip")
    return match


def _header(z: zipfile.ZipFile, member: str) -> set[str]:
    with z.open(member) as handle:
        frame = pd.read_csv(handle, sep=";", encoding="latin1", nrows=0)
    return {str(column).strip().lower() for column in frame.columns}


def probe_susep_conduct_exposure(zip_path: Path | None = None) -> dict[str, Any]:
    path = Path(zip_path or DEFAULT_SES_ZIP)
    if not path.exists():
        return {"state": "source_unavailable", "reason": "ses_basecompleta_missing"}
    if not zipfile.is_zipfile(path):
        return {"state": "source_invalid", "reason": "ses_basecompleta_not_zip"}
    try:
        with zipfile.ZipFile(path) as z:
            insurance_member = _member(z, "Ses_seguros.csv")
            pension_member = _member(z, "Ses_Contrib_Benef.csv")
            insurance_header = _header(z, insurance_member)
            pension_header = _header(z, pension_member)
    except (OSError, ValueError, zipfile.BadZipFile, ConductExposureSourceError) as exc:
        return {
            "state": "source_invalid",
            "reason": "ses_conduct_exposure_unreadable",
            "error": str(exc),
        }

    missing_insurance = sorted(INSURANCE_REQUIRED_COLUMNS - insurance_header)
    missing_pension = sorted(PENSION_REQUIRED_COLUMNS - pension_header)
    missing = {
        "insurance": missing_insurance,
        "pension": missing_pension,
    }
    state = "available" if not missing_insurance and not missing_pension else "schema_incompatible"
    return {
        "state": state,
        "members": {
            "insurance": insurance_member,
            "pension": pension_member,
        },
        "required_columns": {
            "insurance": sorted(INSURANCE_REQUIRED_COLUMNS),
            "pension": sorted(PENSION_REQUIRED_COLUMNS),
        },
        "missing_columns": missing,
        "candidate_exposure_components": [
            "insurance_premium_direct",
            "insurance_premium_earned",
            "pension_contributions",
        ],
        "dimensions": {
            "insurance_branch": "coramo",
            "pension_product": "tipoProd",
        },
    }


def _load_insurance(
    z: zipfile.ZipFile,
    member: str,
    fips: set[str],
) -> tuple[dict[tuple[str, int, int], dict[str, float]], set[int]]:
    aggregate: dict[tuple[str, int, int], dict[str, float]] = defaultdict(
        lambda: {"premium_direct": 0.0, "premium_earned": 0.0, "rows": 0.0}
    )
    periods: set[int] = set()
    with z.open(member) as handle:
        for chunk in pd.read_csv(
            handle,
            sep=";",
            encoding="latin1",
            dtype=str,
            usecols=lambda col: str(col).strip().lower() in INSURANCE_REQUIRED_COLUMNS,
            chunksize=300_000,
            on_bad_lines="skip",
        ):
            chunk.columns = [str(column).strip().lower() for column in chunk.columns]
            for row in chunk.to_dict(orient="records"):
                fip = _canon_fip(row.get("coenti"))
                if fip not in fips:
                    continue
                try:
                    period = int(float(str(row.get("damesano") or "")))
                    branch = int(float(str(row.get("coramo") or "")))
                except ValueError:
                    continue
                direct = _parse_number(row.get("premio_direto"))
                earned = _parse_number(row.get("premio_ganho"))
                if direct is None and earned is None:
                    continue
                key = (fip, period, branch)
                aggregate[key]["premium_direct"] += direct or 0.0
                aggregate[key]["premium_earned"] += earned or 0.0
                aggregate[key]["rows"] += 1.0
                periods.add(period)
    return aggregate, periods


def _load_pension(
    z: zipfile.ZipFile,
    member: str,
    fips: set[str],
) -> tuple[dict[tuple[str, int, str], dict[str, float]], set[int]]:
    aggregate: dict[tuple[str, int, str], dict[str, float]] = defaultdict(
        lambda: {"contributions": 0.0, "rows": 0.0}
    )
    periods: set[int] = set()
    with z.open(member) as handle:
        for chunk in pd.read_csv(
            handle,
            sep=";",
            encoding="latin1",
            dtype=str,
            usecols=lambda col: str(col).strip().lower() in PENSION_REQUIRED_COLUMNS,
            chunksize=300_000,
            on_bad_lines="skip",
        ):
            chunk.columns = [str(column).strip().lower() for column in chunk.columns]
            for row in chunk.to_dict(orient="records"):
                fip = _canon_fip(row.get("coenti"))
                if fip not in fips:
                    continue
                try:
                    period = int(float(str(row.get("damesano") or "")))
                except ValueError:
                    continue
                product = str(row.get("tipoprod") or "").strip() or "unknown"
                contribution = _parse_number(row.get("contrib"))
                if contribution is None:
                    continue
                key = (fip, period, product)
                aggregate[key]["contributions"] += contribution
                aggregate[key]["rows"] += 1.0
                periods.add(period)
    return aggregate, periods


def load_susep_conduct_exposure(
    fip_codes: Iterable[str],
    zip_path: Path | None = None,
) -> dict[str, Any]:
    """Preserve insurance and pension exposure without selecting a denominator."""
    fips = {_canon_fip(value) for value in fip_codes}
    fips.discard("")
    if not fips:
        raise ConductExposureSourceError("at least one FIP code is required")
    path = Path(zip_path or DEFAULT_SES_ZIP)
    probe = probe_susep_conduct_exposure(path)
    if probe.get("state") != "available":
        raise ConductExposureSourceError(f"SES conduct exposure unavailable: {probe}")

    entities: dict[str, Any] = {fip: {"months": {}} for fip in sorted(fips)}
    with zipfile.ZipFile(path) as z:
        insurance, insurance_periods = _load_insurance(
            z,
            probe["members"]["insurance"],
            fips,
        )
        pension, pension_periods = _load_pension(
            z,
            probe["members"]["pension"],
            fips,
        )

    for (fip, period, branch), values in sorted(insurance.items()):
        month = entities[fip]["months"].setdefault(
            period,
            {
                "insurance_premium_direct": 0.0,
                "insurance_premium_earned": 0.0,
                "pension_contributions": 0.0,
                "insurance_branches": {},
                "pension_products": {},
            },
        )
        month["insurance_premium_direct"] += values["premium_direct"]
        month["insurance_premium_earned"] += values["premium_earned"]
        month["insurance_branches"][branch] = dict(values)

    for (fip, period, product), values in sorted(pension.items()):
        month = entities[fip]["months"].setdefault(
            period,
            {
                "insurance_premium_direct": 0.0,
                "insurance_premium_earned": 0.0,
                "pension_contributions": 0.0,
                "insurance_branches": {},
                "pension_products": {},
            },
        )
        month["pension_contributions"] += values["contributions"]
        month["pension_products"][product] = dict(values)

    all_periods = insurance_periods | pension_periods
    return {
        "source": {
            "source_id": "susep_ses_conduct_exposure",
            "authority": "SUSEP",
            "source_file": "BaseCompleta.zip",
            "component_files": {
                "insurance": "Ses_seguros.csv",
                "pension": "Ses_Contrib_Benef.csv",
            },
            "candidate_exposure_components": [
                "insurance_premium_direct",
                "insurance_premium_earned",
                "pension_contributions",
            ],
            "denominator_selected": None,
            "combination_policy": "not_calibrated",
            "scoring": "forbidden_in_source_artifact",
        },
        "reference_periods": {
            "insurance": max(insurance_periods) if insurance_periods else None,
            "pension": max(pension_periods) if pension_periods else None,
        },
        "periods": sorted(all_periods),
        "entities": entities,
    }
