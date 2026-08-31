from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from api.sources.receita_cnpj import RECEITA_CNPJ_OPEN_DATA_URL
from api.v2.build_source_snapshot import (
    SourceSnapshotError,
    _materialize_compatible_receita_cache,
    load_ses_master_companies,
    validate_base_completa,
    validate_listaempresas,
)
from api.v2.refresh_receita_lifecycle import RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION
from api.v2.source_cache import CachedSource


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


def _receita_payload(target_hash: str, *, version: str | None = None) -> dict:
    records = [
        {"cnpj": f"{index:014d}", "cadastral_status": "active"}
        for index in range(300)
    ]
    meta = {
        "target_count": 300,
        "resolved_count": 300,
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


def test_receita_legacy_cache_is_promoted_without_changing_acquisition_time(
    tmp_path: Path,
    monkeypatch,
):
    _disable_golden_check(monkeypatch)
    target_hash = "a" * 64
    legacy = tmp_path / "legacy.json"
    legacy.write_text(
        json.dumps(_receita_payload(target_hash), ensure_ascii=False),
        encoding="utf-8",
    )
    cache = _receita_cache(tmp_path)
    original_fetched_at = "2026-08-31T00:00:00Z"
    cache.store(legacy, fetched_at=original_fetched_at)

    destination = tmp_path / "materialized.json"
    fetched_at, promoted = _materialize_compatible_receita_cache(
        cache=cache,
        destination=destination,
        target_hash=target_hash,
    )

    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert promoted is True
    assert fetched_at == original_fetched_at
    assert (
        payload["meta"]["gate4_cache_contract_version"]
        == RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION
    )
    assert json.loads(cache.metadata_path.read_text(encoding="utf-8"))["fetched_at"] == (
        original_fetched_at
    )


def test_receita_cache_rejects_unknown_contract_version(tmp_path: Path, monkeypatch):
    _disable_golden_check(monkeypatch)
    target_hash = "b" * 64
    source = tmp_path / "future.json"
    source.write_text(
        json.dumps(
            _receita_payload(target_hash, version="v2-receita-lifecycle-999"),
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
            target_hash=target_hash,
        )


def test_receita_cache_rejects_different_target_universe(tmp_path: Path, monkeypatch):
    _disable_golden_check(monkeypatch)
    source = tmp_path / "wrong-universe.json"
    source.write_text(
        json.dumps(
            _receita_payload(
                "c" * 64,
                version=RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION,
            ),
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    cache = _receita_cache(tmp_path)
    cache.store(source, fetched_at="2026-08-31T00:00:00Z")

    with pytest.raises(SourceSnapshotError, match="target-universe hash"):
        _materialize_compatible_receita_cache(
            cache=cache,
            destination=tmp_path / "materialized.json",
            target_hash="d" * 64,
        )
