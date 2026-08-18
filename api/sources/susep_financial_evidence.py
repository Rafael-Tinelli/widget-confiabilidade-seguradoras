from __future__ import annotations

import os
import zipfile
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_SES_ZIP = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")) / "BaseCompleta.zip"

FINANCIAL_SOURCE_ID = "susep_ses_financial_evidence"
FINANCIAL_SOURCE_AUTHORITY = "Superintendência de Seguros Privados (SUSEP)"
FINANCIAL_SOURCE_METHOD = "official_ses_basecompleta_filtered"

# SUSEP, "Índices para Análise Econômico-Financeira das Supervisionadas" (2018),
# still linked from the current Solvência e Contabilidade page in 2026.
SUSEP_INDEX_REFERENCE = (
    "https://www.gov.br/susep/pt-br/arquivos/arquivos-solvencia-supervisao-prudencial/"
    "arquivos/ndicesEcoFinMercadodeSeguros2018.pdf/@@display-file/file"
)

LIQUIDITY_FORMULA_CMPIDS: dict[str, set[int]] = {
    "ILC": {1479, 11160, 351, 1040},
    "ILT": {1479, 11160, 351, 331, 11187, 5503, 1040, 6449},
}

# Candidate operating formulas are retained only to test data availability.
# No metric below receives a score in the financial evidence stage.
OPERATING_FORMULA_CMPIDS: dict[str, set[int]] = {
    "IRETS": {11323, 6183},
    "ISR": {11232, 11248, 4027, 7186, 6238, 6256},
    "IDC": {11237, 11249, 4027, 7186, 6238, 6256},
    "IORDO": {6202, 11231, 6261, 4027, 7186, 6238, 6256},
    "IRRES": {11238, 11250, 4027, 7186, 6238, 6256},
    "IDA": {4069, 4070, 4027, 7186, 6238, 6256},
    "IC": {
        11232,
        11248,
        11237,
        11249,
        6202,
        11231,
        6261,
        11238,
        11250,
        4069,
        4070,
        4027,
        7186,
        6238,
        6256,
    },
    "ICA": {
        11232,
        11248,
        11237,
        11249,
        6202,
        11231,
        6261,
        11238,
        11250,
        4069,
        4070,
        4027,
        7186,
        6238,
        6256,
        6322,
    },
}

BALANCE_CMPIDS_OF_INTEREST = set().union(
    *LIQUIDITY_FORMULA_CMPIDS.values(),
    *OPERATING_FORMULA_CMPIDS.values(),
)


class FinancialEvidenceSourceError(RuntimeError):
    """Raised when the validated SES source cannot support the evidence reader."""


def _canon_fip(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = text.removesuffix(".0")
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _canon_fip_series(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip().str.replace(r"\.0+$", "", regex=True)
    digits = text.str.replace(r"\D+", "", regex=True)
    result = pd.Series([""] * len(digits), index=digits.index, dtype="object")
    valid = digits.str.len().fillna(0).astype(int) > 0
    result.loc[valid] = digits.loc[valid].str.zfill(6)
    return result


def _parse_number_series(series: pd.Series) -> pd.Series:
    """Parse SES numbers without converting missing/invalid data to zero."""
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


def _find_member(z: zipfile.ZipFile, basename: str) -> str:
    target = basename.lower()
    match = next((name for name in z.namelist() if name.lower().endswith(target)), None)
    if not match:
        raise FinancialEvidenceSourceError(f"{basename} not found in BaseCompleta.zip")
    return match


def _header_map(z: zipfile.ZipFile, member: str) -> tuple[list[str], dict[str, str]]:
    with z.open(member) as handle:
        header = list(
            pd.read_csv(handle, sep=";", encoding="latin1", nrows=0).columns
        )
    return header, {column.lower().strip(): column for column in header}


def _require_columns(
    header_map: dict[str, str],
    names: Iterable[str],
    table: str,
) -> list[str]:
    columns: list[str] = []
    missing: list[str] = []
    for name in names:
        column = header_map.get(name.lower())
        if column is None:
            missing.append(name)
        else:
            columns.append(column)
    if missing:
        raise FinancialEvidenceSourceError(
            f"{table} missing required columns: {', '.join(missing)}"
        )
    return columns


def _normalize_chunk_columns(chunk: pd.DataFrame) -> pd.DataFrame:
    """Normalize the column names returned by pandas without reassigning by position.

    pandas.read_csv(usecols=...) returns columns in source-file order. Replacing
    them with the requested usecols order can silently swap semantic fields.
    """
    chunk.columns = [column.lower().strip() for column in chunk.columns]
    return chunk


def _read_capital_history(
    z: zipfile.ZipFile,
    member: str,
    fips: set[str],
) -> tuple[dict[str, dict[int, dict[str, Any]]], set[int], dict[str, int]]:
    _, mapping = _header_map(z, member)
    columns = _require_columns(
        mapping,
        [
            "coenti",
            "damesano",
            "plajustado",
            "margem",
            "pl",
            "AjustesContabeis",
            "AjustesEconomicos",
            "NovoPla",
            "CMR",
        ],
        "Ses_pl_margem.csv",
    )
    history: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    periods: set[int] = set()
    duplicate_counts: dict[str, int] = defaultdict(int)

    with z.open(member) as handle:
        for chunk in pd.read_csv(
            handle,
            sep=";",
            encoding="latin1",
            dtype=str,
            usecols=columns,
            chunksize=300_000,
            on_bad_lines="skip",
        ):
            chunk = _normalize_chunk_columns(chunk)
            chunk["fip"] = _canon_fip_series(chunk["coenti"])
            chunk = chunk[chunk["fip"].isin(fips)]
            if chunk.empty:
                continue
            dates = pd.to_numeric(chunk["damesano"], errors="coerce")
            numeric_columns = [
                "plajustado",
                "margem",
                "pl",
                "ajustescontabeis",
                "ajusteseconomicos",
                "novopla",
                "cmr",
            ]
            parsed = {
                name: _parse_number_series(chunk[name]) for name in numeric_columns
            }
            for index, fip in chunk["fip"].items():
                raw_period = dates.loc[index]
                if pd.isna(raw_period):
                    continue
                period = int(raw_period)
                if period <= 0:
                    continue
                record = {
                    "period": period,
                    "pla_adjusted": _optional_float(parsed["plajustado"].loc[index]),
                    "solvency_margin_legacy": _optional_float(parsed["margem"].loc[index]),
                    "accounting_equity": _optional_float(parsed["pl"].loc[index]),
                    "accounting_adjustments": _optional_float(
                        parsed["ajustescontabeis"].loc[index]
                    ),
                    "economic_adjustments": _optional_float(
                        parsed["ajusteseconomicos"].loc[index]
                    ),
                    "new_pla": _optional_float(parsed["novopla"].loc[index]),
                    "cmr": _optional_float(parsed["cmr"].loc[index]),
                }
                if period in history[fip]:
                    duplicate_counts[fip] += 1
                history[fip][period] = record
                periods.add(period)

    return dict(history), periods, dict(duplicate_counts)


def _optional_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _read_balance_history(
    z: zipfile.ZipFile,
    member: str,
    fips: set[str],
) -> tuple[
    dict[str, set[int]],
    dict[str, dict[int, dict[int, float]]],
    set[int],
    dict[str, int],
    dict[str, dict[int, int]],
]:
    _, mapping = _header_map(z, member)
    columns = _require_columns(
        mapping,
        ["coenti", "damesano", "cmpid", "valor"],
        "SES_Balanco.csv",
    )
    periods_by_fip: dict[str, set[int]] = defaultdict(set)
    values: dict[str, dict[int, dict[int, float]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    periods: set[int] = set()
    duplicate_counts: dict[str, int] = defaultdict(int)
    duplicate_counts_by_period: dict[str, dict[int, int]] = defaultdict(
        lambda: defaultdict(int)
    )

    with z.open(member) as handle:
        for chunk in pd.read_csv(
            handle,
            sep=";",
            encoding="latin1",
            dtype=str,
            usecols=columns,
            chunksize=300_000,
            on_bad_lines="skip",
        ):
            chunk = _normalize_chunk_columns(chunk)
            chunk["fip"] = _canon_fip_series(chunk["coenti"])
            chunk = chunk[chunk["fip"].isin(fips)]
            if chunk.empty:
                continue
            dates = pd.to_numeric(chunk["damesano"], errors="coerce")
            cmpids = pd.to_numeric(chunk["cmpid"], errors="coerce")
            amounts = _parse_number_series(chunk["valor"])

            for index, fip in chunk["fip"].items():
                raw_period = dates.loc[index]
                if pd.isna(raw_period):
                    continue
                period = int(raw_period)
                if period <= 0:
                    continue
                periods_by_fip[fip].add(period)
                periods.add(period)

                raw_cmpid = cmpids.loc[index]
                if pd.isna(raw_cmpid):
                    continue
                cmpid = int(raw_cmpid)
                if cmpid not in BALANCE_CMPIDS_OF_INTEREST:
                    continue
                amount = amounts.loc[index]
                if pd.isna(amount):
                    continue
                existing = values[fip][period].get(cmpid)
                if existing is not None:
                    duplicate_counts[fip] += 1
                    duplicate_counts_by_period[fip][period] += 1
                    values[fip][period][cmpid] = existing + float(amount)
                else:
                    values[fip][period][cmpid] = float(amount)

    return (
        {fip: set(items) for fip, items in periods_by_fip.items()},
        {
            fip: {period: dict(cmpids) for period, cmpids in by_period.items()}
            for fip, by_period in values.items()
        },
        periods,
        dict(duplicate_counts),
        {
            fip: dict(period_counts)
            for fip, period_counts in duplicate_counts_by_period.items()
        },
    )


def _read_insurance_operations_presence(
    z: zipfile.ZipFile,
    member: str,
    fips: set[str],
) -> tuple[dict[str, set[int]], dict[str, set[int]], set[int]]:
    _, mapping = _header_map(z, member)
    columns = _require_columns(
        mapping,
        ["coenti", "damesano", "premio_ganho"],
        "Ses_seguros.csv",
    )
    periods_by_fip: dict[str, set[int]] = defaultdict(set)
    nonzero_periods: dict[str, set[int]] = defaultdict(set)
    periods: set[int] = set()

    with z.open(member) as handle:
        for chunk in pd.read_csv(
            handle,
            sep=";",
            encoding="latin1",
            dtype=str,
            usecols=columns,
            chunksize=300_000,
            on_bad_lines="skip",
        ):
            chunk = _normalize_chunk_columns(chunk)
            chunk["fip"] = _canon_fip_series(chunk["coenti"])
            chunk = chunk[chunk["fip"].isin(fips)]
            if chunk.empty:
                continue
            dates = pd.to_numeric(chunk["damesano"], errors="coerce")
            premiums = _parse_number_series(chunk["premio_ganho"])
            grouped: dict[tuple[str, int], float] = defaultdict(float)
            seen_pairs: set[tuple[str, int]] = set()
            for index, fip in chunk["fip"].items():
                raw_period = dates.loc[index]
                if pd.isna(raw_period):
                    continue
                period = int(raw_period)
                if period <= 0:
                    continue
                periods_by_fip[fip].add(period)
                periods.add(period)
                key = (fip, period)
                seen_pairs.add(key)
                value = premiums.loc[index]
                if not pd.isna(value):
                    grouped[key] += float(value)
            for fip, period in seen_pairs:
                if grouped.get((fip, period), 0.0) != 0.0:
                    nonzero_periods[fip].add(period)

    return (
        {fip: set(items) for fip, items in periods_by_fip.items()},
        {fip: set(items) for fip, items in nonzero_periods.items()},
        periods,
    )


def load_susep_financial_evidence(
    fip_codes: Iterable[str],
    zip_path: Path | None = None,
) -> dict[str, Any]:
    """Load filtered monthly evidence for current insurer FIPs.

    This source layer does not score or judge the financial condition. It only
    preserves evidence needed for later methodology and comparability gates.
    """
    fips = {_canon_fip(value) for value in fip_codes}
    fips.discard("")
    source_path = Path(zip_path or DEFAULT_SES_ZIP)
    if not source_path.exists() or not zipfile.is_zipfile(source_path):
        raise FinancialEvidenceSourceError(
            f"validated BaseCompleta.zip unavailable at {source_path}"
        )

    with zipfile.ZipFile(source_path) as z:
        capital_member = _find_member(z, "Ses_pl_margem.csv")
        balance_member = _find_member(z, "SES_Balanco.csv")
        operations_member = _find_member(z, "Ses_seguros.csv")

        capital, capital_periods, capital_duplicates = _read_capital_history(
            z, capital_member, fips
        )
        (
            balance_periods_by_fip,
            balance_values,
            balance_periods,
            balance_duplicates,
            balance_duplicates_by_period,
        ) = _read_balance_history(z, balance_member, fips)
        (
            operation_periods_by_fip,
            nonzero_premium_periods,
            operation_periods,
        ) = _read_insurance_operations_presence(z, operations_member, fips)

    entities: dict[str, Any] = {}
    for fip in sorted(fips):
        entities[fip] = {
            "capital_history": capital.get(fip, {}),
            "balance_periods": balance_periods_by_fip.get(fip, set()),
            "balance_values": balance_values.get(fip, {}),
            "insurance_operation_periods": operation_periods_by_fip.get(fip, set()),
            "nonzero_premium_periods": nonzero_premium_periods.get(fip, set()),
            "duplicate_capital_rows": capital_duplicates.get(fip, 0),
            "duplicate_balance_cmpid_rows": balance_duplicates.get(fip, 0),
            "duplicate_balance_cmpid_rows_by_period": balance_duplicates_by_period.get(
                fip, {}
            ),
        }

    return {
        "source": {
            "source_id": FINANCIAL_SOURCE_ID,
            "authority": FINANCIAL_SOURCE_AUTHORITY,
            "ingestion_method": FINANCIAL_SOURCE_METHOD,
            "source_file": "BaseCompleta.zip",
            "index_formula_reference": SUSEP_INDEX_REFERENCE,
        },
        "reference_periods": {
            "capital": max(capital_periods) if capital_periods else None,
            "balance": max(balance_periods) if balance_periods else None,
            "insurance_operations": (
                max(operation_periods) if operation_periods else None
            ),
        },
        "entities": entities,
    }
