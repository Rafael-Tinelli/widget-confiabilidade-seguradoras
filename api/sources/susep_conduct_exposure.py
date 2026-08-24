from __future__ import annotations

import os
import zipfile
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_SES_ZIP = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")) / "BaseCompleta.zip"
REQUIRED_COLUMNS = {"damesano", "coenti", "coramo", "premio_direto", "premio_ganho"}


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


def probe_susep_conduct_exposure(zip_path: Path | None = None) -> dict[str, Any]:
    path = Path(zip_path or DEFAULT_SES_ZIP)
    if not path.exists():
        return {"state": "source_unavailable", "reason": "ses_basecompleta_missing"}
    if not zipfile.is_zipfile(path):
        return {"state": "source_invalid", "reason": "ses_basecompleta_not_zip"}
    try:
        with zipfile.ZipFile(path) as z:
            member = _member(z, "Ses_seguros.csv")
            with z.open(member) as handle:
                frame = pd.read_csv(handle, sep=";", encoding="latin1", nrows=0)
    except (OSError, ValueError, zipfile.BadZipFile, ConductExposureSourceError) as exc:
        return {"state": "source_invalid", "reason": "ses_seguros_unreadable", "error": str(exc)}
    header = {str(column).strip().lower() for column in frame.columns}
    missing = sorted(REQUIRED_COLUMNS - header)
    return {
        "state": "available" if not missing else "schema_incompatible",
        "member": member,
        "required_columns": sorted(REQUIRED_COLUMNS),
        "missing_columns": missing,
        "candidate_denominators": ["premium_direct", "premium_earned"],
        "branch_dimension": "coramo",
    }


def load_susep_conduct_exposure(
    fip_codes: Iterable[str],
    zip_path: Path | None = None,
) -> dict[str, Any]:
    """Load monthly premium exposure by FIP and branch without selecting a score."""
    fips = {_canon_fip(value) for value in fip_codes}
    fips.discard("")
    if not fips:
        raise ConductExposureSourceError("at least one FIP code is required")
    path = Path(zip_path or DEFAULT_SES_ZIP)
    probe = probe_susep_conduct_exposure(path)
    if probe.get("state") != "available":
        raise ConductExposureSourceError(f"SES conduct exposure unavailable: {probe}")

    aggregate: dict[tuple[str, int, int], dict[str, float]] = defaultdict(
        lambda: {"premium_direct": 0.0, "premium_earned": 0.0, "rows": 0.0}
    )
    periods: set[int] = set()
    with zipfile.ZipFile(path) as z:
        member = _member(z, "Ses_seguros.csv")
        with z.open(member) as handle:
            for chunk in pd.read_csv(
                handle,
                sep=";",
                encoding="latin1",
                dtype=str,
                usecols=lambda col: str(col).strip().lower() in REQUIRED_COLUMNS,
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

    entities: dict[str, Any] = {fip: {"months": {}} for fip in sorted(fips)}
    for (fip, period, branch), values in sorted(aggregate.items()):
        month = entities[fip]["months"].setdefault(
            period,
            {"premium_direct": 0.0, "premium_earned": 0.0, "branches": {}},
        )
        month["premium_direct"] += values["premium_direct"]
        month["premium_earned"] += values["premium_earned"]
        month["branches"][branch] = dict(values)

    return {
        "source": {
            "source_id": "susep_ses_conduct_exposure",
            "authority": "SUSEP",
            "source_file": "BaseCompleta.zip/Ses_seguros.csv",
            "candidate_denominators": ["premium_direct", "premium_earned"],
            "denominator_selected": None,
            "scoring": "forbidden_in_source_artifact",
        },
        "reference_period": max(periods) if periods else None,
        "periods": sorted(periods),
        "entities": entities,
    }
