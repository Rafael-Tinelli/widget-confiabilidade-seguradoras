from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from api.v2.build_source_snapshot import (
    SourceSnapshotError,
    load_ses_master_companies,
    validate_base_completa,
    validate_listaempresas,
)


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
