import zipfile
from pathlib import Path

import pytest

from api.sources import ses


def test_extract_ses_uses_listaempresas_when_zip_download_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    listaempresas = (
        "CodigoFIP;CNPJ;NomeEntidade\n"
        "5177;61573796000166;ALLIANZ SEGUROS S.A.\n"
    ).encode("latin1")

    monkeypatch.setattr(ses, "CACHE_DIR", tmp_path)

    def fake_download_bytes(_url: str, timeout: int) -> bytes:
        assert timeout == 120
        return listaempresas

    def fail_zip_download(
        _urls: object,
        _destination: Path,
        timeout: int,
    ) -> Path:
        assert timeout == 600
        raise OSError("falha de rede simulada")

    monkeypatch.setattr(ses, "_download_bytes", fake_download_bytes)
    monkeypatch.setattr(ses, "_download_to_file", fail_zip_download)

    meta, companies, financials = ses.extract_ses_master_and_financials()

    assert meta.source == "SUSEP (SES)"
    assert meta.zip_url == ses.SES_ZIP_URL
    assert meta.cias_file == "LISTAEMPRESAS.csv"
    assert meta.seguros_file == "BaseCompleta.zip"

    assert list(companies) == ["005177"]

    company = companies["005177"]
    assert company["name"] == "ALLIANZ SEGUROS S.A."
    assert company["cnpj"] == "61573796000166"
    assert company["premiums"] == 0.0
    assert company["claims"] == 0.0
    assert company["net_worth"] == 0.0

    assert financials == {}


def test_extract_ses_uses_valid_cached_sources_when_network_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    lista_path = tmp_path / "LISTAEMPRESAS.csv"
    lista_path.write_text(
        "CodigoFIP;CNPJ;NomeEntidade\n"
        "5177;61573796000166;ALLIANZ SEGUROS S.A.\n",
        encoding="latin1",
    )

    zip_path = tmp_path / "BaseCompleta.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "Ses_cias.csv",
            "Coenti;Noenti\n"
            "5177;ALLIANZ SEGUROS S.A.\n",
        )

    monkeypatch.setattr(ses, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(ses, "KEEP_ZIP", True)

    def fail_lista_download(_url: str, timeout: int) -> bytes:
        assert timeout == 120
        raise OSError("falha de rede simulada")

    def fail_zip_download(
        _urls: object,
        destination: Path,
        timeout: int,
    ) -> Path:
        assert destination == zip_path
        assert timeout == 600
        raise OSError("falha de rede simulada")

    monkeypatch.setattr(ses, "_download_bytes", fail_lista_download)
    monkeypatch.setattr(ses, "_download_to_file", fail_zip_download)

    meta, companies, financials = ses.extract_ses_master_and_financials()

    assert meta.source == "SUSEP (SES)"
    assert list(companies) == ["005177"]

    company = companies["005177"]
    assert company["name"] == "ALLIANZ SEGUROS S.A."
    assert company["cnpj"] == "61573796000166"

    assert financials["005177"]["premiums"] == 0.0
    assert financials["005177"]["claims"] == 0.0
    assert financials["005177"]["net_worth"] == 0.0
    assert financials["61573796000166"] == financials["005177"]

    assert zip_path.exists()
