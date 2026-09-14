from __future__ import annotations

import json
from pathlib import Path

import pytest

from api.sources.receita_cnpj import RECEITA_CNPJ_OPEN_DATA_URL
from api.v2.build_source_snapshot import (
    SourceSnapshotError,
    _project_receita_cache_to_current_population,
    _receita_snapshot,
    _validate_receita_cache_payload,
)
from api.v2.generation import BuildContext
from api.v2.refresh_receita_lifecycle import regulatory_target_universe_hash
from api.v2.source_cache import CachedSource


def _cnpj(index: int) -> str:
    return f"{index + 10_000_000_000_000:014d}"


def _entities(count: int) -> list[dict[str, str]]:
    return [
        {"cnpj": _cnpj(index), "legal_name": f"Entity {index}"}
        for index in range(count)
    ]


def _record(index: int) -> dict[str, object]:
    return {
        "cnpj": _cnpj(index),
        "legal_name": f"Entity {index}",
        "cadastral_status": "active",
        "status_date": "2026-01-01",
        "status_reason": None,
        "source_authority": "Receita Federal do Brasil",
        "source_document": "Dados Abertos do CNPJ / Estabelecimentos",
        "source_mode": "official_open_data_bulk",
        "observed_at": "2026-08-01",
        "source_period": "2026-08",
        "source_url": "https://arquivos.receitafederal.gov.br/2026-08/Estabelecimentos0.zip",
        "source_file": "Estabelecimentos0.zip",
        "raw_status_code": "02",
        "raw_reason_code": "00",
    }


def _legacy_payload(resolved: int) -> dict[str, object]:
    records = [_record(index) for index in range(resolved)]
    return {
        "artifact": "v2_receita_cnpj_lifecycle",
        "generated_at": "2026-09-08T12:00:00Z",
        "status": "ok",
        "source": {
            "authority": "Receita Federal do Brasil",
            "dataset": "Cadastro Nacional da Pessoa Jurídica (CNPJ) - Dados Abertos",
            "public_share_url": "https://arquivos.receitafederal.gov.br/index.php/s/test",
            "release_url": "https://arquivos.receitafederal.gov.br/public.php/dav/files/test/2026-08/",
            "reference_period": "2026-08",
            "ingestion_method": "official_nextcloud_webdav_bulk_filtered",
            "retrieved_at": "2026-09-08T12:00:00Z",
            "resource_metadata": [],
        },
        "meta": {
            "target_count": resolved,
            "resolved_count": resolved,
            "unresolved_count": 0,
            "files_scanned": ["Estabelecimentos0.zip"],
            "available_establishment_files": ["Estabelecimentos0.zip"],
            "target_universe_hash": "e" * 64,
            "target_universe_hash_method": "legacy_regulatory_attributes",
            "gate4_cache_contract_version": "v2-receita-lifecycle-1",
        },
        "unresolved_cnpjs": [],
        "records": records,
    }


def _disable_golden_check(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "api.v2.refresh_receita_lifecycle.load_verified_lifecycle_snapshot",
        lambda: [],
    )


def _context() -> BuildContext:
    return BuildContext(
        build_id="v2-test-partial-receita",
        source_head_sha="a" * 40,
        generated_at="2026-09-14T21:00:00Z",
        workflow_run_id="test",
        workflow_run_attempt=1,
    )


def test_three_new_susep_cnpjs_become_explicit_unresolved_crosschecks(monkeypatch):
    _disable_golden_check(monkeypatch)
    entities = _entities(313)
    payload = _legacy_payload(310)
    target_hash = regulatory_target_universe_hash(entities)

    assert _validate_receita_cache_payload(
        payload,
        entities=entities,
        target_hash=target_hash,
    ) is True

    projected = _project_receita_cache_to_current_population(
        payload,
        entities=entities,
        target_hash=target_hash,
    )

    assert projected["status"] == "partial"
    assert projected["meta"]["target_count"] == 313
    assert projected["meta"]["resolved_count"] == 310
    assert projected["meta"]["unresolved_count"] == 3
    assert projected["unresolved_cnpjs"] == [_cnpj(310), _cnpj(311), _cnpj(312)]
    assert projected["meta"]["gate4_cache_contract_version"] == "v2-receita-lifecycle-2"


def test_material_receita_coverage_loss_still_fails_closed(monkeypatch):
    _disable_golden_check(monkeypatch)
    entities = _entities(330)
    payload = _legacy_payload(310)
    target_hash = regulatory_target_universe_hash(entities)

    with pytest.raises(SourceSnapshotError, match="below 95.00%"):
        _validate_receita_cache_payload(
            payload,
            entities=entities,
            target_hash=target_hash,
        )


def test_manifest_outage_uses_partial_cache_as_stale_not_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _disable_golden_check(monkeypatch)
    entities = _entities(313)
    classification = {"entities": entities}
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cached = CachedSource(
        source_id="receita_cnpj_lifecycle",
        source_url=RECEITA_CNPJ_OPEN_DATA_URL,
        content_path=cache_dir / "receita_cnpj_lifecycle.json",
        metadata_path=cache_dir / "receita_cnpj_lifecycle.meta.json",
    )
    source = tmp_path / "legacy.json"
    source.write_text(json.dumps(_legacy_payload(310)), encoding="utf-8")
    cached.store(source, fetched_at="2026-09-08T12:00:00Z")

    monkeypatch.setattr(
        "api.v2.build_source_snapshot.discover_latest_release",
        lambda: (_ for _ in ()).throw(ConnectionError("official manifest unavailable")),
    )

    destination = tmp_path / "receita.json"
    result = _receita_snapshot(
        classification=classification,
        destination=destination,
        cache_dir=cache_dir,
        existing_snapshot=tmp_path / "missing.json",
        context=_context(),
    )

    lineage = result.observation.to_lineage(_context())
    materialized = json.loads(destination.read_text(encoding="utf-8"))

    assert result.used_cache is True
    assert lineage.state == "stale"
    assert lineage.freshness_method == "validated_cache_fallback"
    assert materialized["meta"]["unresolved_count"] == 3
    assert materialized["unresolved_cnpjs"] == [_cnpj(310), _cnpj(311), _cnpj(312)]
    assert "official manifest unavailable" in (result.current_error or "")
    assert "legal crosscheck unavailable for 3 current CNPJs" in (
        result.observation.state_reason or ""
    )
