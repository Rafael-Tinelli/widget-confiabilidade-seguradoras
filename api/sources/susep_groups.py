from __future__ import annotations

import os
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd

from api.v2.identity import canonical_fip_code

DEFAULT_SES_CACHE_DIR = Path("data/raw/ses")
SES_CACHE_DIR = Path(os.getenv("SES_CACHE_DIR", str(DEFAULT_SES_CACHE_DIR)))


class SusepGroupSourceError(ValueError):
    """Raised when the SES group table cannot be read safely."""


def _find_cias_file(z: zipfile.ZipFile) -> str:
    for name in z.namelist():
        if name.lower().endswith("ses_cias.csv"):
            return name
    raise SusepGroupSourceError("Ses_cias.csv not found in BaseCompleta.zip")


def load_susep_economic_groups(
    zip_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Read FIP -> economic-group relationships from official SES master data."""
    path = zip_path or (SES_CACHE_DIR / "BaseCompleta.zip")
    if not path.exists() or not zipfile.is_zipfile(path):
        raise SusepGroupSourceError(f"Valid BaseCompleta.zip not found at {path}")

    with zipfile.ZipFile(path) as z:
        filename = _find_cias_file(z)
        with z.open(filename) as handle:
            frame = pd.read_csv(
                handle,
                sep=";",
                encoding="latin1",
                dtype=str,
                on_bad_lines="skip",
            )

    frame.columns = [str(col).strip().casefold() for col in frame.columns]
    required = {"coenti", "noenti", "cogrupo", "nogrupo"}
    if not required.issubset(frame.columns):
        missing = sorted(required - set(frame.columns))
        raise SusepGroupSourceError(f"Ses_cias.csv missing group columns: {missing}")

    records: list[dict[str, Any]] = []
    seen_fips: set[str] = set()
    for _, row in frame.iterrows():
        fip = canonical_fip_code(row.get("coenti"))
        if not fip:
            continue
        if fip in seen_fips:
            raise SusepGroupSourceError(f"Duplicate FIP in Ses_cias.csv: {fip}")
        seen_fips.add(fip)

        group_code = str(row.get("cogrupo") or "").strip()
        group_name = str(row.get("nogrupo") or "").strip()
        if group_code.lower() in {"nan", "none"}:
            group_code = ""
        if group_name.lower() in {"nan", "none"}:
            group_name = ""

        records.append(
            {
                "fip_code": fip,
                "legal_name": str(row.get("noenti") or "").strip(),
                "group_code": group_code or None,
                "group_name": group_name or None,
                "source": "SUSEP SES / Ses_cias.csv",
            }
        )

    return records
