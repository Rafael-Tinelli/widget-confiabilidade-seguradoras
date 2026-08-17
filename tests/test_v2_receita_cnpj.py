import json
from pathlib import Path

import pytest

from api.sources.receita_cnpj import (
    ReceitaLifecycleError,
    load_lifecycle_records,
    load_verified_lifecycle_snapshot,
    normalize_receita_lifecycle_record,
    validate_verified_snapshot_against_bulk,
)


def test_normalizes_closed_incorporation_record():
    record = normalize_receita_lifecycle_record(
        {
            "cnpj": "33.061.813/0001-40",
            "legal_name": "PRUDENTIAL DO BRASIL SEGUROS DE VIDA S.A.",
            "cadastral_status": "BAIXADA",
            "status_date": "01/11/2024",
            "status_reason": "Incorporação",
            "observed_at": "17/08/2026",
        }
    )

    assert record["cnpj"] == "33061813000140"
    assert record["cadastral_status"] == "closed"
    assert record["status_date"] == "2024-11-01"
    assert record["status_reason"] == "incorporation"
    assert record["observed_at"] == "2026-08-17"


def test_closed_record_requires_date():
    with pytest.raises(ReceitaLifecycleError, match="requires status_date"):
        normalize_receita_lifecycle_record(
            {
                "cnpj": "33061813000140",
                "legal_name": "TESTE S.A.",
                "cadastral_status": "BAIXADA",
            }
        )


def test_snapshot_rejects_duplicate_cnpj(tmp_path: Path):
    path = tmp_path / "snapshot.json"
    path.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "cnpj": "33061813000140",
                        "legal_name": "A",
                        "cadastral_status": "BAIXADA",
                        "status_date": "2024-11-01",
                    },
                    {
                        "cnpj": "33061813000140",
                        "legal_name": "B",
                        "cadastral_status": "BAIXADA",
                        "status_date": "2024-11-01",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReceitaLifecycleError, match="Duplicate"):
        load_verified_lifecycle_snapshot(path)


def _record(source_mode: str, *, status_date: str = "2024-11-01") -> dict:
    return {
        "cnpj": "33061813000140",
        "legal_name": "PRUDENTIAL DO BRASIL SEGUROS DE VIDA S.A.",
        "cadastral_status": "BAIXADA",
        "status_date": status_date,
        "status_reason": "Incorporação",
        "observed_at": "2026-08-17",
        "source_mode": source_mode,
    }


def test_bulk_snapshot_becomes_primary_and_verified_snapshot_is_golden_check(
    tmp_path: Path,
):
    verified = tmp_path / "verified.json"
    bulk = tmp_path / "bulk.json"
    verified.write_text(json.dumps({"records": [_record("verified_snapshot")]}))
    bulk.write_text(json.dumps({"records": [_record("official_open_data_bulk")]}))

    records = load_lifecycle_records(bulk, verified)

    assert records[0]["source_mode"] == "official_open_data_bulk"


def test_bulk_snapshot_conflict_with_verified_case_fails_closed(tmp_path: Path):
    verified = tmp_path / "verified.json"
    bulk = tmp_path / "bulk.json"
    verified.write_text(json.dumps({"records": [_record("verified_snapshot")]}))
    bulk.write_text(
        json.dumps(
            {
                "records": [
                    _record("official_open_data_bulk", status_date="2024-12-01")
                ]
            }
        )
    )

    with pytest.raises(ReceitaLifecycleError, match="conflicts with verified"):
        load_lifecycle_records(bulk, verified)


def test_verified_snapshot_is_explicit_fallback_when_bulk_is_absent(tmp_path: Path):
    verified = tmp_path / "verified.json"
    missing_bulk = tmp_path / "missing.json"
    verified.write_text(json.dumps({"records": [_record("verified_snapshot")]}))

    records = load_lifecycle_records(missing_bulk, verified)

    assert records[0]["source_mode"] == "verified_snapshot"


def test_direct_golden_validation_reports_missing_verified_cnpj():
    verified = [normalize_receita_lifecycle_record(_record("verified_snapshot"))]

    with pytest.raises(ReceitaLifecycleError, match="missing from bulk"):
        validate_verified_snapshot_against_bulk([], verified)
