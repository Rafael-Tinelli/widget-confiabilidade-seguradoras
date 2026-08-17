import json
from pathlib import Path

import pytest

from api.sources.receita_cnpj import (
    ReceitaLifecycleError,
    load_verified_lifecycle_snapshot,
    normalize_receita_lifecycle_record,
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
