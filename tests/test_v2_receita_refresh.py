import json
from pathlib import Path

import pytest

from api.v2.refresh_receita_lifecycle import (
    ReceitaRefreshValidationError,
    validate_refresh_payload,
    write_snapshot_atomic,
)


def _payload(target=300, resolved=300):
    records = [
        {"cnpj": f"{i:014d}", "cadastral_status": "active"}
        for i in range(resolved)
    ]
    return {
        "source": {
            "authority": "Receita Federal do Brasil",
            "ingestion_method": "official_nextcloud_webdav_bulk_filtered",
            "reference_period": "2026-08",
        },
        "meta": {
            "target_count": target,
            "resolved_count": resolved,
            "unresolved_count": target - resolved,
            "files_scanned": ["Estabelecimentos0.zip"],
        },
        "records": records,
    }


def _disable_golden_check(monkeypatch):
    monkeypatch.setattr(
        "api.v2.refresh_receita_lifecycle.load_verified_lifecycle_snapshot",
        lambda: [],
    )


def test_refresh_validation_rejects_low_coverage(monkeypatch):
    _disable_golden_check(monkeypatch)
    with pytest.raises(ReceitaRefreshValidationError, match="coverage"):
        validate_refresh_payload(_payload(target=300, resolved=270))


def test_refresh_validation_accepts_high_coverage(monkeypatch):
    _disable_golden_check(monkeypatch)
    validate_refresh_payload(_payload(target=300, resolved=290))


def test_refresh_validation_rejects_material_drop(monkeypatch):
    _disable_golden_check(monkeypatch)
    previous = [{"cnpj": str(i)} for i in range(310)]
    with pytest.raises(ReceitaRefreshValidationError, match="dropped"):
        validate_refresh_payload(
            _payload(target=300, resolved=290),
            previous_records=previous,
        )


def test_atomic_writer_replaces_only_final_path(tmp_path: Path):
    output = tmp_path / "receita.json"
    payload = {"records": [{"cnpj": "123"}]}
    write_snapshot_atomic(payload, output)
    assert json.loads(output.read_text(encoding="utf-8")) == payload
    assert not output.with_suffix(".json.tmp").exists()
