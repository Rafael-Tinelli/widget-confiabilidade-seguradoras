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
    monkeypatch.setattr(
        ses,
        "_download_bytes",
        lambda _url, _timeout: listaempresas,
    )

    def fail_zip_download(
        _urls: object,
        _destination: Path,
        _timeout: int,
    ) -> Path:
        raise OSError("falha de rede simulada")

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
