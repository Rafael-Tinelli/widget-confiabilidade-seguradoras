from __future__ import annotations

import ast
import importlib.util
import io
import subprocess
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


def _zip_bytes(payload: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("fixture.csv", payload)
    return buffer.getvalue()


class _FakeResponse:
    def __init__(self, payload: bytes):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size=8192):
        del chunk_size
        yield self.payload


class _FakeSession:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.calls = 0
        self.auth = None

        class _Cookies:
            def clear(self):
                return None

        self.cookies = _Cookies()

    def get(self, url, **kwargs):
        del url, kwargs
        self.calls += 1
        return _FakeResponse(self.payload)


def test_remote_worker_cache_reuses_fingerprint_and_rejects_same_size_tamper(
    tmp_path,
) -> None:
    worker = _load_worker_module()
    official = _zip_bytes(b"AAAA")
    tampered = _zip_bytes(b"BBBB")
    assert len(official) == len(tampered)

    session = _FakeSession(official)
    destination = tmp_path / "Motivos.zip"

    worker.download_zip(
        session,
        "https://example.invalid/Motivos.zip",
        destination,
        len(official),
        '"etag-1"',
        "Mon, 14 Sep 2026 15:06:40 GMT",
    )
    assert session.calls == 1

    # Same remote fingerprint + same local SHA-256: no GET.
    worker.download_zip(
        session,
        "https://example.invalid/Motivos.zip",
        destination,
        len(official),
        '"etag-1"',
        "Mon, 14 Sep 2026 15:06:40 GMT",
    )
    assert session.calls == 1

    # Replace with another valid ZIP of exactly the same byte length.
    destination.write_bytes(tampered)
    assert zipfile.is_zipfile(destination)
    assert destination.stat().st_size == len(official)

    worker.download_zip(
        session,
        "https://example.invalid/Motivos.zip",
        destination,
        len(official),
        '"etag-1"',
        "Mon, 14 Sep 2026 15:06:40 GMT",
    )
    assert session.calls == 2
    assert destination.read_bytes() == official

    # A changed remote fingerprint also invalidates otherwise intact bytes.
    worker.download_zip(
        session,
        "https://example.invalid/Motivos.zip",
        destination,
        len(official),
        '"etag-2"',
        "Mon, 14 Sep 2026 15:06:40 GMT",
    )
    assert session.calls == 3


def test_ssh_base_uses_keepalive_and_connection_multiplexing(tmp_path) -> None:
    config = bridge.ReceitaRemoteConfig(
        host="example.test",
        user="tester",
        port=2222,
        key_file=tmp_path / "key",
        known_hosts_file=tmp_path / "known_hosts",
        remote_root=".bridge",
    )
    control = tmp_path / "control.sock"
    command = bridge._ssh_base(config, control_path=control)
    rendered = " ".join(command)

    assert "ConnectTimeout=20" in rendered
    assert "ConnectionAttempts=3" in rendered
    assert "TCPKeepAlive=yes" in rendered
    assert "ServerAliveInterval=20" in rendered
    assert "ServerAliveCountMax=9" in rendered
    assert "ControlMaster=auto" in rendered
    assert "ControlPersist=600" in rendered
    assert f"ControlPath={control}" in rendered


def test_transport_retries_only_transient_ssh_failures(monkeypatch) -> None:
    calls = []
    sleeps = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        if len(calls) < 3:
            raise bridge.ReceitaRemoteBridgeError(
                "Receita remote command failed: "
                "kex_exchange_identification: read: Connection reset by peer"
            )
        return subprocess.CompletedProcess(command, 0, "ok\n", "")

    monkeypatch.setattr(bridge, "_run", fake_run)
    monkeypatch.setattr(bridge.time, "sleep", lambda value: sleeps.append(value))

    result = bridge._run_transport(
        ["ssh", "example"],
        timeout=30,
        attempts=4,
    )

    assert result.stdout == "ok\n"
    assert len(calls) == 3
    assert sleeps == [2, 4]


def test_transport_does_not_retry_semantic_remote_failure(monkeypatch) -> None:
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        raise bridge.ReceitaRemoteBridgeError(
            "Receita remote command failed: worker validation failed"
        )

    monkeypatch.setattr(bridge, "_run", fake_run)
    monkeypatch.setattr(
        bridge.time,
        "sleep",
        lambda value: (_ for _ in ()).throw(
            AssertionError(f"unexpected sleep: {value}")
        ),
    )

    try:
        bridge._run_transport(
            ["ssh", "example"],
            timeout=30,
            attempts=6,
        )
    except bridge.ReceitaRemoteBridgeError as exc:
        assert "worker validation failed" in str(exc)
    else:
        raise AssertionError("semantic remote failure should propagate")

    assert len(calls) == 1


def test_remote_worker_deadline_allows_long_foreground_execution() -> None:
    assert bridge._REMOTE_DEADLINE_SECONDS >= 18_000


def test_remote_call_uses_foreground_worker_not_detachment() -> None:
    source = Path("api/v2/receita_remote_bridge.py").read_text(encoding="utf-8")
    assert "execute_worker_foreground" in source
    assert '"--daemonize"' not in source
    assert '"--heartbeat-file"' not in source
    assert "_poll_remote_worker" not in source
    assert "_detached_launch_command" not in source


def test_remote_worker_has_no_daemonization_contract() -> None:
    source = Path("ops/hostgator/receita_remote_worker.py").read_text(encoding="utf-8")
    assert "def daemonize(" not in source
    assert "start_heartbeat_process" not in source
    assert "120-second CPU limit per process" in source


def test_full_generation_acceptance_mode_requires_fresh_receita() -> None:
    workflow = Path(
        ".github/workflows/v2-gate4-full-generation-proof.yml"
    ).read_text(encoding="utf-8")
    assert "require_receita_fresh:" in workflow
    assert "V2_REQUIRE_RECEITA_FRESH" in workflow
    assert "receita_cnpj_lifecycle" in workflow
    assert "receita_consumer_identity" in workflow
    assert '{"current_fetch", "current_validation"}' in workflow
    assert "Receita acceptance proof failed" in workflow


def test_remote_diagnostics_are_preserved_on_bridge_failure() -> None:
    source = Path("api/v2/receita_remote_bridge.py").read_text(encoding="utf-8")
    assert "completed_successfully = False" in source
    assert "completed_successfully = True" in source
    assert "preserve_remote_diagnostics" in source


def test_remote_identity_worker_is_cpu_sharded() -> None:
    source = Path("ops/hostgator/receita_remote_worker.py").read_text(encoding="utf-8")
    assert "IDENTITY_CHUNK_BYTES = 256 * 1024 * 1024" in source
    assert "identity_company_chunk" in source
    assert "identity_establishment_chunk" in source
    assert "_materialize_zip_chunks" in source
    assert '["unzip", "-p"' in source
    assert '"split",' in source
    assert "_run_identity_chunk" in source
    assert "120-second CPU limit per process" in source


def test_remote_lifecycle_worker_is_cpu_sharded() -> None:
    source = Path("ops/hostgator/receita_remote_worker.py").read_text(encoding="utf-8")
    assert "lifecycle_establishment_chunk" in source
    assert "def _scan_lifecycle_chunk(" in source
    lifecycle = source.split("def lifecycle_extract(", 1)[1].split(
        "IDENTITY_CHUNK_BYTES", 1
    )[0]
    assert "_scan_zip_via_chunk_workers(" in lifecycle
    assert 'mode="lifecycle_establishment_chunk"' in lifecycle
    assert "RECEITA_REMOTE_CHUNK" in source
