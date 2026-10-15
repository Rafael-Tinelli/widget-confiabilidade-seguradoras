from __future__ import annotations

import json
import zipfile
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from api.sources.receita_cnpj import RECEITA_CNPJ_OPEN_DATA_URL
from api.v2.build_source_snapshot import (
    SourceSnapshotError,
    _assert_no_stale_sandbox_override,
    _materialize_compatible_receita_cache,
    _receita_snapshot,
    load_ses_master_companies,
    validate_base_completa,
    validate_listaempresas,
)
from api.v2.generation import BuildContext
from api.v2.refresh_receita_lifecycle import (
    RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION,
    regulatory_target_universe_hash,
    write_snapshot_atomic,
)
from api.v2.source_cache import AcquisitionResult, CachedSource
from api.v2.source_snapshot import SourceObservation


def _write_lista(path: Path) -> None:
    path.write_text(
        "CodigoFIP;CNPJ;NomeEntidade\n"
        "5177;61573796000166;ALLIANZ SEGUROS S.A.\n"
        "5886;61198164000160;PORTO SEGURO COMPANHIA DE SEGUROS GERAIS\n",
        encoding="latin1",
    )


def _write_base(path: Path, *, companies: int = 120) -> None:
    rows = ["Coenti;Noenti"]
    rows.append("5177;ALLIANZ SEGUROS S.A.")
    rows.append("5886;PORTO SEGURO COMPANHIA DE SEGUROS GERAIS")
    for index in range(companies - 2):
        code = 10000 + index
        rows.append(f"{code};SEGURADORA TESTE {index} S.A.")

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("Ses_cias.csv", "\n".join(rows) + "\n")
        archive.writestr("Ses_pl_margem.csv", "Coenti;damesano;NovoPla;CMR\n")
        archive.writestr("SES_Balanco.csv", "Coenti;damesano;conta;valor\n")


def _receita_payload(
    target_hash: str,
    *,
    version: str | None = None,
    cnpjs: list[str] | None = None,
) -> dict:
    resolved_cnpjs = cnpjs or [
        f"{index + 10000000000000:014d}" for index in range(300)
    ]
    records = [
        {
            "cnpj": cnpj,
            "legal_name": f"SEGURADORA CACHE {index} S.A.",
            "cadastral_status": "active",
        }
        for index, cnpj in enumerate(resolved_cnpjs)
    ]
    meta = {
        "target_count": len(records),
        "resolved_count": len(records),
        "unresolved_count": 0,
        "files_scanned": ["Estabelecimentos0.zip"],
        "target_universe_hash": target_hash,
    }
    if version is not None:
        meta["gate4_cache_contract_version"] = version
    return {
        "source": {
            "authority": "Receita Federal do Brasil",
            "ingestion_method": "official_nextcloud_webdav_bulk_filtered",
            "reference_period": "2026-08",
        },
        "meta": meta,
        "records": records,
    }


def _disable_golden_check(monkeypatch) -> None:
    monkeypatch.setattr(
        "api.v2.refresh_receita_lifecycle.load_verified_lifecycle_snapshot",
        list,
    )


def _receita_cache(tmp_path: Path) -> CachedSource:
    return CachedSource(
        source_id="receita_cnpj_lifecycle",
        source_url=RECEITA_CNPJ_OPEN_DATA_URL,
        content_path=tmp_path / "receita-cache.json",
        metadata_path=tmp_path / "receita-cache.meta.json",
    )


def _classification(count: int = 300) -> dict:
    return {
        "entities": [
            {
                "entity_id": f"fip:{index:06d}",
                "cnpj": f"{index + 10000000000000:014d}",
                "legal_name": f"SEGURADORA TESTE {index} S.A.",
                "entity_type": "insurer",
                "regulatory_subtype": None,
                "regulatory_regime": "ordinary",
                "regulatory_status": "active_licensed",
            }
            for index in range(count)
        ]
    }


def _classification_cnpjs(classification: dict) -> list[str]:
    return [str(entity["cnpj"]) for entity in classification["entities"]]


def _context() -> BuildContext:
    return BuildContext(
        build_id="v2-test",
        source_head_sha="a" * 40,
        generated_at="2026-08-31T00:00:00Z",
        workflow_run_id="test",
        workflow_run_attempt=1,
    )



def test_stale_sandbox_cannot_override_fresh_current_license(tmp_path: Path):
    licensed_path = tmp_path / "licensed.json"
    sandbox_path = tmp_path / "sandbox.json"
    licensed_path.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "fip_code": "001988",
                        "cnpj": "47006254000180",
                        "legal_name": "LTI SEGUROS S.A.",
                        "entity_type": "insurer",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    sandbox_path.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "cnpj": "47006254000180",
                        "legal_name": "LTI Seguros S.A.",
                        "regulatory_status": "temporary_authorized",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    context = _context()
    licensed = AcquisitionResult(
        observation=SourceObservation(
            source_id="susep_licensed_entities",
            source_url="https://www2.susep.gov.br/licensed",
            snapshot_path=licensed_path,
            fetched_at="2026-09-21T15:57:08Z",
            current_fetch_succeeded=True,
        ),
        used_cache=False,
    )
    sandbox = AcquisitionResult(
        observation=SourceObservation(
            source_id="susep_sandbox",
            source_url="https://www.gov.br/susep/sandbox",
            snapshot_path=sandbox_path,
            fetched_at="2026-09-15T02:26:32Z",
            current_fetch_succeeded=False,
            state_reason="SandboxSourceError: returned too few records: 0",
        ),
        used_cache=True,
    )

    with pytest.raises(SourceSnapshotError, match="stale Sandbox snapshot overlaps"):
        _assert_no_stale_sandbox_override(
            licensed=licensed,
            sandbox=sandbox,
            licensed_path=licensed_path,
            sandbox_path=sandbox_path,
            context=context,
        )


def test_stale_sandbox_without_active_overlap_does_not_block(tmp_path: Path):
    licensed_path = tmp_path / "licensed.json"
    sandbox_path = tmp_path / "sandbox.json"
    licensed_path.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "fip_code": "001988",
                        "cnpj": "47006254000180",
                        "legal_name": "LTI SEGUROS S.A.",
                        "entity_type": "insurer",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    sandbox_path.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "cnpj": "39768897000133",
                        "legal_name": "COOVER SEGURADORA S.A.",
                        "regulatory_status": "sandbox_authorization_cancelled",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    context = _context()
    licensed = AcquisitionResult(
        observation=SourceObservation(
            source_id="susep_licensed_entities",
            source_url="https://www2.susep.gov.br/licensed",
            snapshot_path=licensed_path,
            fetched_at="2026-09-21T15:57:08Z",
            current_fetch_succeeded=True,
        ),
        used_cache=False,
    )
    sandbox = AcquisitionResult(
        observation=SourceObservation(
            source_id="susep_sandbox",
            source_url="https://www.gov.br/susep/sandbox",
            snapshot_path=sandbox_path,
            fetched_at="2026-09-15T02:26:32Z",
            current_fetch_succeeded=False,
        ),
        used_cache=True,
    )

    _assert_no_stale_sandbox_override(
        licensed=licensed,
        sandbox=sandbox,
        licensed_path=licensed_path,
        sandbox_path=sandbox_path,
        context=context,
    )

def test_gate4_ses_master_is_derived_only_from_materialized_files(tmp_path: Path):
    lista = tmp_path / "LISTAEMPRESAS.csv"
    base = tmp_path / "BaseCompleta.zip"
    _write_lista(lista)
    _write_base(base)

    validate_listaempresas(lista)
    validate_base_completa(base)
    companies = load_ses_master_companies(
        base_completa=base,
        listaempresas=lista,
    )

    assert len(companies) == 120
    assert companies["005177"]["name"] == "ALLIANZ SEGUROS S.A."
    assert companies["005177"]["cnpj"] == "61573796000166"
    assert companies["005886"]["cnpj"] == "61198164000160"
    assert companies["005177"]["premiums"] == 0.0


def test_base_completa_validation_requires_prudential_sources(tmp_path: Path):
    base = tmp_path / "BaseCompleta.zip"
    with zipfile.ZipFile(base, "w") as archive:
        archive.writestr("Ses_cias.csv", "Coenti;Noenti\n5177;ALLIANZ\n")

    with pytest.raises(SourceSnapshotError, match="prudential capital"):
        validate_base_completa(base)


def test_listaempresas_validation_requires_cnpj_mapping(tmp_path: Path):
    lista = tmp_path / "LISTAEMPRESAS.csv"
    lista.write_text("CodigoFIP;NomeEntidade\n5177;ALLIANZ\n", encoding="latin1")

    with pytest.raises(SourceSnapshotError, match="CNPJ"):
        validate_listaempresas(lista)


def test_target_hash_ignores_mutable_regulatory_attributes():
    classification = _classification()
    changed = deepcopy(classification)
    changed["entities"][0]["legal_name"] = "NOVO NOME S.A."
    changed["entities"][0]["regulatory_status"] = "special_regime"
    changed["entities"][0]["regulatory_regime"] = "special"
    changed["entities"][0]["entity_type"] = "open_pension_entity"

    assert regulatory_target_universe_hash(classification["entities"]) == (
        regulatory_target_universe_hash(changed["entities"])
    )


def test_target_hash_changes_when_cnpj_population_changes():
    classification = _classification()
    reduced = {"entities": classification["entities"][:-1]}

    assert regulatory_target_universe_hash(classification["entities"]) != (
        regulatory_target_universe_hash(reduced["entities"])
    )


def test_receita_legacy_v1_cache_is_promoted_by_current_cnpj_coverage(
    tmp_path: Path,
    monkeypatch,
):
    _disable_golden_check(monkeypatch)
    classification = _classification()
    current_hash = regulatory_target_universe_hash(classification["entities"])
    legacy = tmp_path / "legacy.json"
    legacy.write_text(
        json.dumps(
            _receita_payload(
                "a" * 64,
                version="v2-receita-lifecycle-1",
                cnpjs=_classification_cnpjs(classification),
            ),
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    cache = _receita_cache(tmp_path)
    original_fetched_at = "2026-08-31T00:00:00Z"
    cache.store(legacy, fetched_at=original_fetched_at)

    destination = tmp_path / "materialized.json"
    fetched_at, migrated = _materialize_compatible_receita_cache(
        cache=cache,
        destination=destination,
        entities=classification["entities"],
        target_hash=current_hash,
    )

    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert migrated is True
    assert fetched_at == original_fetched_at
    assert (
        payload["meta"]["gate4_cache_contract_version"]
        == RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION
    )
    assert payload["meta"]["target_universe_hash"] == current_hash
    assert payload["meta"]["target_universe_hash_method"] == "sorted_normalized_cnpjs_v1"
    assert payload["meta"]["gate4_cache_migration"]["from_contract"] == (
        "v2-receita-lifecycle-1"
    )
    assert json.loads(cache.metadata_path.read_text(encoding="utf-8"))["fetched_at"] == (
        original_fetched_at
    )


def test_receita_legacy_cache_projects_extra_removed_cnpjs(
    tmp_path: Path,
    monkeypatch,
):
    _disable_golden_check(monkeypatch)
    classification = _classification()
    current_cnpjs = _classification_cnpjs(classification)
    cached_cnpjs = [*current_cnpjs, "99999999999999"]
    source = tmp_path / "superset.json"
    source.write_text(
        json.dumps(
            _receita_payload(
                "b" * 64,
                version="v2-receita-lifecycle-1",
                cnpjs=cached_cnpjs,
            )
        ),
        encoding="utf-8",
    )
    cache = _receita_cache(tmp_path)
    cache.store(source, fetched_at="2026-08-31T00:00:00Z")

    destination = tmp_path / "materialized.json"
    _, migrated = _materialize_compatible_receita_cache(
        cache=cache,
        destination=destination,
        entities=classification["entities"],
        target_hash=regulatory_target_universe_hash(classification["entities"]),
    )

    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert migrated is True
    assert payload["meta"]["target_count"] == len(current_cnpjs)
    assert payload["meta"]["resolved_count"] == len(current_cnpjs)
    assert {record["cnpj"] for record in payload["records"]} == set(current_cnpjs)


def test_receita_cache_projects_small_unresolved_current_cnpj_delta(
    tmp_path: Path,
    monkeypatch,
):
    _disable_golden_check(monkeypatch)
    classification = _classification()
    current_cnpjs = _classification_cnpjs(classification)
    cached_cnpjs = current_cnpjs[:-1]
    source = tmp_path / "missing-current.json"
    source.write_text(
        json.dumps(
            _receita_payload(
                "c" * 64,
                version="v2-receita-lifecycle-1",
                cnpjs=cached_cnpjs,
            )
        ),
        encoding="utf-8",
    )
    cache = _receita_cache(tmp_path)
    cache.store(source, fetched_at="2026-08-31T00:00:00Z")

    destination = tmp_path / "materialized.json"
    _, migrated = _materialize_compatible_receita_cache(
        cache=cache,
        destination=destination,
        entities=classification["entities"],
        target_hash=regulatory_target_universe_hash(classification["entities"]),
    )

    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert migrated is True
    assert payload["status"] == "partial"
    assert payload["meta"]["target_count"] == len(current_cnpjs)
    assert payload["meta"]["resolved_count"] == len(cached_cnpjs)
    assert payload["meta"]["unresolved_count"] == 1
    assert payload["unresolved_cnpjs"] == [current_cnpjs[-1]]


def test_receita_cache_rejects_unknown_contract_version(tmp_path: Path, monkeypatch):
    _disable_golden_check(monkeypatch)
    classification = _classification()
    target_hash = regulatory_target_universe_hash(classification["entities"])
    source = tmp_path / "future.json"
    source.write_text(
        json.dumps(
            _receita_payload(
                target_hash,
                version="v2-receita-lifecycle-999",
                cnpjs=_classification_cnpjs(classification),
            ),
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    cache = _receita_cache(tmp_path)
    cache.store(source, fetched_at="2026-08-31T00:00:00Z")

    with pytest.raises(SourceSnapshotError, match="unsupported.*cache contract"):
        _materialize_compatible_receita_cache(
            cache=cache,
            destination=tmp_path / "materialized.json",
            entities=classification["entities"],
            target_hash=target_hash,
        )


def test_incompatible_receita_cache_triggers_fresh_refresh(tmp_path: Path, monkeypatch):
    classification = _classification()
    target_hash = regulatory_target_universe_hash(classification["entities"])
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cache = CachedSource(
        source_id="receita_cnpj_lifecycle",
        source_url=RECEITA_CNPJ_OPEN_DATA_URL,
        content_path=cache_dir / "receita_cnpj_lifecycle.json",
        metadata_path=cache_dir / "receita_cnpj_lifecycle.meta.json",
    )
    future = tmp_path / "future.json"
    future.write_text(
        json.dumps(
            _receita_payload(
                target_hash,
                version="v2-receita-lifecycle-999",
                cnpjs=_classification_cnpjs(classification),
            ),
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    cache.store(future, fetched_at="2026-08-31T00:00:00Z")

    monkeypatch.setattr(
        "api.v2.build_source_snapshot.discover_latest_release",
        lambda: SimpleNamespace(period="2026-08"),
    )

    def fake_refresh(*, output, release, entities):
        assert release.period == "2026-08"
        assert entities == classification["entities"]
        payload = _receita_payload(
            target_hash,
            version=RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION,
            cnpjs=_classification_cnpjs(classification),
        )
        write_snapshot_atomic(payload, output)
        return payload

    monkeypatch.setattr(
        "api.v2.build_source_snapshot.refresh_receita_lifecycle",
        fake_refresh,
    )
    destination = tmp_path / "receita.json"
    result = _receita_snapshot(
        classification=classification,
        destination=destination,
        cache_dir=cache_dir,
        existing_snapshot=tmp_path / "missing-existing.json",
        context=_context(),
    )

    assert result.used_cache is False
    lineage = result.observation.to_lineage(_context())
    assert lineage.state == "fresh"
    assert lineage.freshness_method == "current_fetch"
    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert (
        payload["meta"]["gate4_cache_contract_version"]
        == RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION
    )


def test_receita_unavailable_migrates_legacy_cache_when_cnpjs_are_covered(
    tmp_path: Path,
    monkeypatch,
):
    _disable_golden_check(monkeypatch)
    classification = _classification()
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cache = CachedSource(
        source_id="receita_cnpj_lifecycle",
        source_url=RECEITA_CNPJ_OPEN_DATA_URL,
        content_path=cache_dir / "receita_cnpj_lifecycle.json",
        metadata_path=cache_dir / "receita_cnpj_lifecycle.meta.json",
    )
    stale = tmp_path / "stale.json"
    stale.write_text(
        json.dumps(
            _receita_payload(
                "e" * 64,
                version="v2-receita-lifecycle-1",
                cnpjs=_classification_cnpjs(classification),
            ),
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    cache.store(stale, fetched_at="2026-08-31T00:00:00Z")

    monkeypatch.setattr(
        "api.v2.build_source_snapshot.discover_latest_release",
        lambda: (_ for _ in ()).throw(ConnectionError("official manifest unavailable")),
    )
    destination = tmp_path / "receita.json"
    context = _context()
    result = _receita_snapshot(
        classification=classification,
        destination=destination,
        cache_dir=cache_dir,
        existing_snapshot=tmp_path / "missing-existing.json",
        context=context,
    )

    assert result.used_cache is True
    assert result.current_error is not None
    assert "ConnectionError: official manifest unavailable" in result.current_error
    lineage = result.observation.to_lineage(context)
    assert lineage.state == "stale"
    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert (
        payload["meta"]["gate4_cache_contract_version"]
        == RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION
    )
    assert "compatible cache migrated to v2" in (result.observation.state_reason or "")


def test_receita_unavailable_preserves_small_missing_current_cnpj_as_partial(
    tmp_path: Path,
    monkeypatch,
):
    _disable_golden_check(monkeypatch)
    classification = _classification()
    current_cnpjs = _classification_cnpjs(classification)
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cache = CachedSource(
        source_id="receita_cnpj_lifecycle",
        source_url=RECEITA_CNPJ_OPEN_DATA_URL,
        content_path=cache_dir / "receita_cnpj_lifecycle.json",
        metadata_path=cache_dir / "receita_cnpj_lifecycle.meta.json",
    )
    stale = tmp_path / "stale.json"
    stale.write_text(
        json.dumps(
            _receita_payload(
                "e" * 64,
                version="v2-receita-lifecycle-1",
                cnpjs=current_cnpjs[:-1],
            ),
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    cache.store(stale, fetched_at="2026-08-31T00:00:00Z")

    monkeypatch.setattr(
        "api.v2.build_source_snapshot.discover_latest_release",
        lambda: (_ for _ in ()).throw(ConnectionError("official manifest unavailable")),
    )
    destination = tmp_path / "receita.json"
    context = _context()
    result = _receita_snapshot(
        classification=classification,
        destination=destination,
        cache_dir=cache_dir,
        existing_snapshot=tmp_path / "missing-existing.json",
        context=context,
    )

    assert result.used_cache is True
    assert result.current_error is not None
    assert "ConnectionError: official manifest unavailable" in result.current_error
    lineage = result.observation.to_lineage(context)
    assert lineage.state == "stale"
    assert lineage.freshness_method == "validated_cache_fallback"
    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert payload["status"] == "partial"
    assert payload["meta"]["unresolved_count"] == 1
    assert payload["unresolved_cnpjs"] == [current_cnpjs[-1]]
    assert "legal crosscheck unavailable for 1 current CNPJs" in (
        result.observation.state_reason or ""
    )
