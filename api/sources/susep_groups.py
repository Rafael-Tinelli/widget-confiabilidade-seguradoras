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


def _is_specific_group(code: str | None, name: str | None) -> bool:
    if not code or not name:
        return False
    return (code, name.upper()) not in GENERIC_GROUP_BUCKETS


def _compress_group_history(rows: pd.DataFrame) -> list[dict[str, Any]]:
    """Compress monthly SES rows into contiguous group-label periods."""
    history: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    for _, row in rows.sort_values("damesano").iterrows():
        period = _clean(row.get("damesano"))
        code = _clean(row.get("cogrupo")) or None
        name = _clean(row.get("nogrupo")) or None
        key = (code, name)

        if current and (current["group_code"], current["group_name"]) == key:
            current["to_period"] = period
            continue

        current = {
            "group_code": code,
            "group_name": name,
            "from_period": period,
            "to_period": period,
            "is_specific_group": _is_specific_group(code, name),
        }
        history.append(current)

    return history


def load_susep_economic_groups(
    zip_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Read official SES economic-group history and latest observation by FIP.

    ``Ses_grupos_economicos.csv`` is monthly. The returned record contains the
    latest observation plus a compact transition history. Generic buckets such
    as INDEPENDENTE and OUTROS GRUPOS are retained as source observations but
    marked non-specific so they never become false shared corporate groups.
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
    records: list[dict[str, Any]] = []
    seen_fips: set[str] = set()

    for _, rows in frame.groupby("coenti", sort=False):
        row = rows.iloc[-1]
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
        records.append(
            {
                "fip_code": fip,
                "legal_name": _clean(row.get("noenti")),
                "group_code": group_code,
                "group_name": group_name,
                "observed_period": _clean(row.get("damesano")),
                "is_specific_group": _is_specific_group(group_code, group_name),
                "group_history": _compress_group_history(rows),
                "source": "SUSEP SES / Ses_grupos_economicos.csv",
            }
        )

    return records
