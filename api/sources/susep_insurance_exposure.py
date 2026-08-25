from __future__ import annotations

import os
import zipfile
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_SES_ZIP = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")) / "BaseCompleta.zip"
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


def _number(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


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
        "explicitly_excluded_domains": ["private_pension", "capitalization"],
    }


def load_susep_insurance_exposure(
    fip_codes: Iterable[str],
    zip_path: Path | None = None,
) -> dict[str, Any]:
    """Load insurance premiums only; never reads pension or capitalization files."""
    fips = {_canon_fip(value) for value in fip_codes}
    fips.discard("")
    if not fips:
        raise InsuranceExposureSourceError("at least one FIP code is required")

    path = Path(zip_path or DEFAULT_SES_ZIP)
    probe = probe_susep_insurance_exposure(path)
    if probe.get("state") != "available":
        raise InsuranceExposureSourceError(f"SES insurance exposure unavailable: {probe}")

    aggregate: dict[tuple[str, int, int], dict[str, float]] = defaultdict(
        lambda: {"premium_direct": 0.0, "premium_earned": 0.0, "rows": 0.0}
    )
    periods: set[int] = set()

    with zipfile.ZipFile(path) as z, z.open(str(probe["member"])) as handle:
        for chunk in pd.read_csv(
            handle,
            sep=";",
            encoding="latin1",
            dtype=str,
            usecols=lambda column: str(column).strip().lower() in REQUIRED_COLUMNS,
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
                direct = _number(row.get("premio_direto"))
                earned = _number(row.get("premio_ganho"))
                if direct is None and earned is None:
                    continue
                key = (fip, period, branch)
                aggregate[key]["premium_direct"] += direct or 0.0
                aggregate[key]["premium_earned"] += earned or 0.0
                aggregate[key]["rows"] += 1.0
                periods.add(period)

    entities: dict[str, Any] = {fip: {"months": {}} for fip in sorted(fips)}
    for (fip, period, branch), values in sorted(aggregate.items()):
        month = entities[fip]["months"].setdefault(
            period,
            {
                "insurance_premium_direct": 0.0,
                "insurance_premium_earned": 0.0,
                "insurance_branches": {},
            },
        )
        month["insurance_premium_direct"] += values["premium_direct"]
        month["insurance_premium_earned"] += values["premium_earned"]
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
            "explicitly_excluded_domains": ["private_pension", "capitalization"],
            "scoring": "forbidden_in_source_artifact",
        },
        "periods": sorted(periods),
        "reference_period": max(periods) if periods else None,
        "entities": entities,
    }
