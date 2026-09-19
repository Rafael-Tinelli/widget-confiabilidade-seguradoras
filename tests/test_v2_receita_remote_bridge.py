from __future__ import annotations

import ast
import importlib.util
import json
import zipfile
from pathlib import Path

from api.sources.receita_cnpj_bulk import ReceitaOpenDataRelease
from api.sources.receita_cnpj_identity import (
    _build_query_indexes,
    _business_core,
    _match_name_to_queries,
    _surface_name,
)
from api.v2 import receita_remote_bridge as bridge


def _load_worker_module():
    path = Path("ops/hostgator/receita_remote_worker.py")
    spec = importlib.util.spec_from_file_location("receita_remote_worker_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module




def test_remote_worker_syntax_remains_python39_compatible() -> None:
    source = Path("ops/hostgator/receita_remote_worker.py").read_text(encoding="utf-8")
    ast.parse(source, filename="receita_remote_worker.py", feature_version=(3, 9))

def _release() -> ReceitaOpenDataRelease:
    return ReceitaOpenDataRelease(
        base_url="https://arquivos.receitafederal.gov.br/index.php/s/current",
        release_url=(
            "https://arquivos.receitafederal.gov.br/public.php/dav/files/"
            "current/Dados/Cadastros/CNPJ/2026-09/"
        ),
        period="2026-09",
        establishment_files=("Estabelecimentos0.zip",),
        resource_metadata=(),
    )


def test_remote_worker_name_matching_stays_equivalent_to_canonical_code() -> None:
    worker = _load_worker_module()
    providers = [
        "CIA ITAU DE CAPITALIZACAO",
        "Previsul Seguradora S.A.",
        "Yelum Seguros S.A.",
        "IRB (SEG) Corporate Seguradora S.A.",
    ]
    canonical_indexes = _build_query_indexes(providers)
    worker_indexes = worker.build_query_indexes(providers)

    names = [
        "CIA ITAU DE CAPITALIZACAO",
        "PREVISUL SEGURADORA S/A",
        "Yelum Seguros S.A.",
        "IRB (SEG) CORPORATE SEGURADORA S.A.",
        "Nome sem correspondência",
    ]
    for name in names:
        assert worker.surface_name(name) == _surface_name(name)
        assert worker.business_core(name) == _business_core(name)
        assert {
            tuple(item)
            for item in worker.match_name_to_queries(
                name, worker_indexes, "legal_name"
            )
        } == set(
            _match_name_to_queries(
                name,
                canonical_indexes,
                prefix="legal_name",
            )
        )


def test_remote_lifecycle_extract_is_canonicalized_locally(monkeypatch) -> None:
    release = _release()
    monkeypatch.setattr(
        bridge,
        "_remote_call",
        lambda mode, payload=None: {
            "artifact": "v2_receita_remote_lifecycle_extract",
            "generated_at": "2026-09-18T18:00:00Z",
            "source": {
                "authority": "Receita Federal do Brasil",
                "dataset": "Cadastro Nacional da Pessoa Jurídica (CNPJ) - Dados Abertos",
                "public_share_url": release.base_url,
                "release_url": release.release_url,
                "reference_period": release.period,
                "ingestion_method": "official_nextcloud_webdav_bulk_filtered",
                "retrieved_at": "2026-09-18T18:00:00Z",
                "resource_metadata": [],
            },
            "meta": {
                "target_count": 1,
                "resolved_count": 1,
                "unresolved_count": 0,
                "files_scanned": ["Estabelecimentos0.zip"],
            },
            "unresolved_cnpjs": [],
            "raw_records": [
                {
                    "cnpj": "12345678000195",
                    "legal_name": "EMPRESA TESTE S.A.",
                    "cadastral_status": "ATIVA",
                    "status_date": "20260901",
                    "status_reason": None,
                    "source_authority": "Receita Federal do Brasil",
                    "source_document": "Dados Abertos do CNPJ / Estabelecimentos",
                    "source_mode": "official_open_data_bulk",
                    "observed_at": "2026-09-18",
                    "source_period": release.period,
                    "source_url": release.release_url + "Estabelecimentos0.zip",
                    "source_file": "Estabelecimentos0.zip",
                    "raw_status_code": "02",
                    "raw_reason_code": "00",
                }
            ],
        },
    )

    payload = bridge.refresh_lifecycle_remote(
        [{"cnpj": "12.345.678/0001-95", "legal_name": "EMPRESA TESTE S.A."}],
        release=release,
    )

    assert payload["status"] == "ok"
    assert payload["records"][0]["cnpj"] == "12345678000195"
    assert payload["records"][0]["cadastral_status"] == "active"
    assert payload["records"][0]["status_date"] == "2026-09-01"
    assert payload["meta"]["resolved_count"] == 1


def test_remote_identity_extract_is_revalidated_and_finalized_locally(monkeypatch) -> None:
    release = _release()
    row = [
        "12345678",
        "0001",
        "95",
        "1",
        "EMPRESA TESTE",
        "02",
        "20260901",
        "00",
        "",
        "",
        "20200101",
        "6511101",
        "",
    ]
    monkeypatch.setattr(
        bridge,
        "_remote_call",
        lambda mode, payload=None: {
            "artifact": "v2_receita_remote_identity_extract",
            "generated_at": "2026-09-18T18:00:00Z",
            "source": {
                "authority": "Receita Federal do Brasil",
                "dataset": "Cadastro Nacional da Pessoa Jurídica (CNPJ) - Dados Abertos",
                "public_share_url": release.base_url,
                "release_url": release.release_url,
                "reference_period": release.period,
                "ingestion_method": "official_nextcloud_webdav_bulk_filtered_identity",
                "retrieved_at": "2026-09-18T18:00:00Z",
                "resource_metadata": [],
            },
            "cnae_map": {"6511101": "Seguros de vida"},
            "nature_map": {"2054": "Sociedade Anônima Fechada"},
            "companies_by_base": {
                "12345678": {
                    "cnpj_base": "12345678",
                    "legal_name_receita": "EMPRESA TESTE SEGURADORA S.A.",
                    "legal_nature_code": "2054",
                    "legal_nature": "Sociedade Anônima Fechada",
                }
            },
            "selected_establishments": [
                {
                    "row": row,
                    "provider_matches": [
                        ["empresa teste", "legal_name_business_core"],
                        ["empresa teste", "trade_name_surface_exact"],
                        ["empresa teste", "trade_name_business_core"],
                    ],
                }
            ],
            "company_files_scanned": ["Empresas0.zip"],
            "establishment_files_scanned": ["Estabelecimentos0.zip"],
        },
    )

    payload = bridge.build_identity_remote(
        [
            {
                "cnpj": "12.345.678/0001-95",
                "entity_id": "test-entity",
                "legal_name": "EMPRESA TESTE SEGURADORA S.A.",
            }
        ],
        ["Empresa Teste"],
        release=release,
    )

    assert payload["canonical_records"][0]["entity_id"] == "test-entity"
    assert payload["canonical_records"][0]["primary_cnae"] == "Seguros de vida"
    match = payload["provider_matches"][0]
    assert match["candidate_state"] == "unique_candidate"
    assert match["candidates"][0]["cnpj"] == "12345678000195"
    assert match["candidates"][0]["entity_id"] == "test-entity"
    assert (
        match["candidates"][0]["project_legal_name"]
        == "EMPRESA TESTE SEGURADORA S.A."
    )


def test_remote_worker_cache_manifest_reuses_and_invalidates_same_size_tamper(
    tmp_path,
    monkeypatch,
) -> None:
    worker = _load_worker_module()
    source = tmp_path / "source.zip"
    with zipfile.ZipFile(source, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("data.csv", "a;b\n1;2\n")

    cache_root = tmp_path / "cache" / "2026-09"
    cache_root.mkdir(parents=True)
    destination = cache_root / "Motivos.zip"
    destination.write_bytes(source.read_bytes())
    metadata = {
        "name": "Motivos.zip",
        "size": destination.stat().st_size,
        "etag": '"stable-etag"',
        "last_modified": "Mon, 14 Sep 2026 15:06:40 GMT",
    }

    class _NoNetworkSession:
        def get(self, *args, **kwargs):
            raise AssertionError("network must not be used for adopted/reused cache")

    session = _NoNetworkSession()
    first = worker.download_zip(
        session,
        "https://example.invalid/Motivos.zip",
        destination,
        metadata["size"],
        reference_period="2026-09",
        resource_metadata=metadata,
    )
    assert first == destination

    manifest_path = cache_root / worker.CACHE_MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = manifest["files"]["Motivos.zip"]
    assert entry["provenance"] == "adopted_existing_current_manifest"

    worker.download_zip(
        session,
        "https://example.invalid/Motivos.zip",
        destination,
        metadata["size"],
        reference_period="2026-09",
        resource_metadata=metadata,
    )

    data = bytearray(destination.read_bytes())
    data[len(data) // 2] ^= 0xFF
    destination.write_bytes(data)
    assert destination.stat().st_size == metadata["size"]

    class _DownloadResponse:
        status_code = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size):
            yield source.read_bytes()

    class _DownloadSession:
        auth = None

        class _Cookies:
            @staticmethod
            def clear():
                return None

        cookies = _Cookies()

        def get(self, *args, **kwargs):
            return _DownloadResponse()

    repaired = worker.download_zip(
        _DownloadSession(),
        "https://example.invalid/Motivos.zip",
        destination,
        metadata["size"],
        reference_period="2026-09",
        resource_metadata=metadata,
    )
    assert repaired.read_bytes() == source.read_bytes()
    refreshed = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert refreshed["files"]["Motivos.zip"]["provenance"] == (
        "downloaded_current_manifest"
    )
