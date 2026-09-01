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

FINANCIAL_SOURCE_ID = "susep_ses_financial_evidence"
FINANCIAL_SOURCE_AUTHORITY = "Superintendência de Seguros Privados (SUSEP)"
FINANCIAL_SOURCE_METHOD = "official_ses_basecompleta_filtered"
SES_CSV_CHUNK_ROWS = 300_000

SES_NUMBER_MISSING_TOKENS = {"", "nan", "none", "<na>"}
SES_FIP_PATTERN = r"[0-9]{1,6}"
SES_INTEGER_PATTERN = r"[0-9]+"
SES_NUMBER_DECIMAL_DOT_PATTERN = (
    r"[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?"
)
SES_NUMBER_DECIMAL_COMMA_PATTERN = (
    r"[+-]?(?:[0-9]+|[0-9]{1,3}(?:\.[0-9]{3})+),[0-9]+"
    r"(?:[eE][+-]?[0-9]+)?"
)

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
    if not text.isdigit() or len(text) > 6:
        return ""
    return text.zfill(6)


def _invalid_examples(text: pd.Series, invalid: pd.Series) -> str:
    return ", ".join(
        repr(value) for value in text[invalid].drop_duplicates().head(3).tolist()
    )


def _parse_fip_series(series: pd.Series, *, table: str) -> pd.Series:
    text = series.astype("string").str.strip()
    valid = text.notna() & text.str.fullmatch(SES_FIP_PATTERN, na=False)
    if not bool(valid.all()):
        raise FinancialEvidenceSourceError(
            f"invalid coenti value in {table}: {_invalid_examples(text, ~valid)}"
        )
    return text.str.zfill(6).astype("object")


def _parse_integer_series(
    series: pd.Series,
    *,
    field: str,
    table: str,
    minimum: int = 0,
) -> pd.Series:
    text = series.astype("string").str.strip()
    valid = text.notna() & text.str.fullmatch(SES_INTEGER_PATTERN, na=False)
    if not bool(valid.all()):
        raise FinancialEvidenceSourceError(
            f"invalid {field} value in {table}: {_invalid_examples(text, ~valid)}"
        )
    values = pd.Series((int(value) for value in text), index=text.index, dtype="object")
    in_range = values.map(lambda value: value >= minimum)
    if not bool(in_range.all()):
        raise FinancialEvidenceSourceError(
            f"invalid {field} value in {table}: "
            f"{_invalid_examples(text, ~in_range)}"
        )
    return values


def _parse_period_series(series: pd.Series, *, table: str) -> pd.Series:
    text = series.astype("string").str.strip()
    values = _parse_integer_series(
        series,
        field="damesano",
        table=table,
    )
    valid = values.map(
        lambda value: 1000 <= value // 100 <= 9999 and 1 <= value % 100 <= 12
    )
    if not bool(valid.all()):
        raise FinancialEvidenceSourceError(
            f"invalid damesano value in {table}: {_invalid_examples(text, ~valid)}"
        )
    return values


def _parse_number_series(
    series: pd.Series,
    *,
    field: str,
    table: str,
) -> pd.Series:
    """Parse SES numbers without deleting characters from malformed tokens."""
    text = series.astype("string").str.strip()
    missing = text.isna() | text.str.lower().isin(SES_NUMBER_MISSING_TOKENS)
    has_comma = text.str.contains(",", na=False)
    valid = missing | (
        has_comma
        & text.str.fullmatch(SES_NUMBER_DECIMAL_COMMA_PATTERN, na=False)
    ) | (
        ~has_comma & text.str.fullmatch(SES_NUMBER_DECIMAL_DOT_PATTERN, na=False)
    )
    if not bool(valid.all()):
        raise FinancialEvidenceSourceError(
            f"invalid {field} value in {table}: {_invalid_examples(text, ~valid)}"
        )

    normalized = text.mask(missing)
    normalized.loc[has_comma] = (
        normalized.loc[has_comma]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    parsed = pd.to_numeric(normalized, errors="raise")
    finite = parsed.map(lambda value: pd.isna(value) or math.isfinite(float(value)))
    if not bool(finite.all()):
        raise FinancialEvidenceSourceError(
            f"non-finite {field} value in {table}: "
            f"{_invalid_examples(text, ~finite)}"
        )
    return parsed


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
            chunksize=SES_CSV_CHUNK_ROWS,
            on_bad_lines="error",
        ):
            chunk = _normalize_chunk_columns(chunk)
            chunk = chunk[[column.lower().strip() for column in columns]]
            chunk["fip"] = _parse_fip_series(
                chunk["coenti"],
                table="Ses_pl_margem.csv",
            )
            chunk = chunk[chunk["fip"].isin(fips)]
            if chunk.empty:
                continue
            dates = _parse_period_series(
                chunk["damesano"],
                table="Ses_pl_margem.csv",
            )
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
                name: _parse_number_series(
                    chunk[name],
                    field=name,
                    table="Ses_pl_margem.csv",
                )
                for name in numeric_columns
            }
            for index, fip in chunk["fip"].items():
                period = int(dates.loc[index])
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
            chunksize=SES_CSV_CHUNK_ROWS,
            on_bad_lines="error",
        ):
            chunk = _normalize_chunk_columns(chunk)
            chunk = chunk[[column.lower().strip() for column in columns]]
            chunk["fip"] = _parse_fip_series(
                chunk["coenti"],
                table="SES_Balanco.csv",
            )
            chunk = chunk[chunk["fip"].isin(fips)]
            if chunk.empty:
                continue
            dates = _parse_period_series(
                chunk["damesano"],
                table="SES_Balanco.csv",
            )
            cmpids = _parse_integer_series(
                chunk["cmpid"],
                field="cmpid",
                table="SES_Balanco.csv",
                minimum=1,
            )
            amounts = _parse_number_series(
                chunk["valor"],
                field="valor",
                table="SES_Balanco.csv",
            )

            for index, fip in chunk["fip"].items():
                period = int(dates.loc[index])
                periods_by_fip[fip].add(period)
                periods.add(period)

                cmpid = int(cmpids.loc[index])
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
    premium_totals: dict[tuple[str, int], float] = defaultdict(float)

    with z.open(member) as handle:
        for chunk in pd.read_csv(
            handle,
            sep=";",
            encoding="latin1",
            dtype=str,
            chunksize=SES_CSV_CHUNK_ROWS,
            on_bad_lines="error",
        ):
            chunk = _normalize_chunk_columns(chunk)
            chunk = chunk[[column.lower().strip() for column in columns]]
            chunk["fip"] = _parse_fip_series(
                chunk["coenti"],
                table="Ses_seguros.csv",
            )
            chunk = chunk[chunk["fip"].isin(fips)]
            if chunk.empty:
                continue
            dates = _parse_period_series(
                chunk["damesano"],
                table="Ses_seguros.csv",
            )
            premiums = _parse_number_series(
                chunk["premio_ganho"],
                field="premio_ganho",
                table="Ses_seguros.csv",
            )
            for index, fip in chunk["fip"].items():
                period = int(dates.loc[index])
                periods_by_fip[fip].add(period)
                periods.add(period)
                key = (fip, period)
                value = premiums.loc[index]
                if not pd.isna(value):
                    premium_totals[key] += float(value)

    for (fip, period), total in premium_totals.items():
        if total != 0.0:
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
    fips: set[str] = set()
    invalid_fips: list[Any] = []
    for value in fip_codes:
        fip = _canon_fip(value)
        if fip:
            fips.add(fip)
        else:
            invalid_fips.append(value)
    if invalid_fips:
        examples = ", ".join(repr(value) for value in invalid_fips[:3])
        raise FinancialEvidenceSourceError(f"invalid requested FIP code: {examples}")
    if not fips:
        raise FinancialEvidenceSourceError("at least one valid FIP code is required")
    source_path = Path(zip_path or DEFAULT_SES_ZIP)
    if not source_path.exists() or not zipfile.is_zipfile(source_path):
        raise FinancialEvidenceSourceError(
            f"validated BaseCompleta.zip unavailable at {source_path}"
        )

    try:
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
    except FinancialEvidenceSourceError:
        raise
    except (OSError, ValueError, zipfile.BadZipFile) as exc:
        raise FinancialEvidenceSourceError(
            f"unable to parse validated BaseCompleta.zip: {exc}"
        ) from exc

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
            "malformed_row_policy": "fail_closed_not_skipped",
            "key_parsing_policy": "strict_integer_keys_and_valid_aaaamm_periods",
            "numeric_parsing_policy": "strict_finite_decimal_or_scientific_notation",
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
