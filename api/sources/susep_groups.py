from __future__ import annotations

import os
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd

from api.v2.identity import canonical_fip_code

DEFAULT_SES_CACHE_DIR = Path("data/raw/ses")
SES_CACHE_DIR = Path(os.getenv("SES_CACHE_DIR", str(DEFAULT_SES_CACHE_DIR)))
GENERIC_GROUP_BUCKETS = {
    ("01225", "INDEPENDENTE"),
    ("99999", "OUTROS GRUPOS"),
}


class SusepGroupSourceError(ValueError):
    """Raised when the SES economic-group history cannot be read safely."""


def _find_group_file(z: zipfile.ZipFile) -> str:
    for name in z.namelist():
        if name.lower().endswith("ses_grupos_economicos.csv"):
            return name
    raise SusepGroupSourceError("Ses_grupos_economicos.csv not found in BaseCompleta.zip")


def _clean(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.casefold() in {"nan", "none"} else text


def load_susep_economic_groups(
    zip_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Read the latest official SES economic-group observation for each FIP.

    ``Ses_grupos_economicos.csv`` is a monthly history. Only the most recent
    observation for each FIP is returned. Generic buckets such as INDEPENDENTE
    and OUTROS GRUPOS are retained as source observations but explicitly marked
    as non-specific so the relationship layer does not turn them into false
    shared corporate groups.
    """
    path = zip_path or (SES_CACHE_DIR / "BaseCompleta.zip")
    if not path.exists() or not zipfile.is_zipfile(path):
        raise SusepGroupSourceError(f"Valid BaseCompleta.zip not found at {path}")

    with zipfile.ZipFile(path) as z:
        filename = _find_group_file(z)
        with z.open(filename) as handle:
            frame = pd.read_csv(
                handle,
                sep=";",
                encoding="latin1",
                dtype=str,
                on_bad_lines="skip",
            )

    frame.columns = [str(col).strip().casefold() for col in frame.columns]
    required = {"damesano", "coenti", "noenti", "cogrupo", "nogrupo"}
    if not required.issubset(frame.columns):
        missing = sorted(required - set(frame.columns))
        raise SusepGroupSourceError(
            f"Ses_grupos_economicos.csv missing columns: {missing}"
        )

    frame = frame.copy()
    for column in required:
        frame[column] = frame[column].map(_clean)
    frame = frame[frame["damesano"].str.fullmatch(r"\d{6}", na=False)]
    if frame.empty:
        raise SusepGroupSourceError("SES economic-group history contains no valid periods")

    frame = frame.sort_values(["coenti", "damesano"])
    latest = frame.groupby("coenti", as_index=False, sort=False).tail(1)

    records: list[dict[str, Any]] = []
    seen_fips: set[str] = set()
    for _, row in latest.iterrows():
        fip = canonical_fip_code(row.get("coenti"))
        if not fip:
            continue
        if fip in seen_fips:
            raise SusepGroupSourceError(
                f"Duplicate latest FIP in Ses_grupos_economicos.csv: {fip}"
            )
        seen_fips.add(fip)

        group_code = _clean(row.get("cogrupo")) or None
        group_name = _clean(row.get("nogrupo")) or None
        is_specific_group = bool(group_code and group_name) and (
            group_code,
            group_name.upper(),
        ) not in GENERIC_GROUP_BUCKETS

        records.append(
            {
                "fip_code": fip,
                "legal_name": _clean(row.get("noenti")),
                "group_code": group_code,
                "group_name": group_name,
                "observed_period": _clean(row.get("damesano")),
                "is_specific_group": is_specific_group,
                "source": "SUSEP SES / Ses_grupos_economicos.csv",
            }
        )

    return records
