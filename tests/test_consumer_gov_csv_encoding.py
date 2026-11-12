from __future__ import annotations

from pathlib import Path

import pytest

from api import build_consumidor_gov as build
from api.sources import consumer_gov_direct as direct
from api.sources.consumer_gov_csv import detect_csv_encoding


def _rows(path: Path) -> list[dict[str, str]]:
    return list(build._iter_rows(path))


def test_late_non_utf8_byte_does_not_replay_earlier_rows(tmp_path: Path) -> None:
    path = tmp_path / "late.csv"
    path.write_bytes(
        b"Nome Fantasia;Valor\n"
        + b"Seguradora Exemplo;A\n" * 5001
        + "Última Seguradora;B\n".encode("latin1")
    )

    assert detect_csv_encoding(path) == "latin1"
    rows = _rows(path)
    assert len(rows) == 5002
    assert sum(row["Nome Fantasia"] == "Seguradora Exemplo" for row in rows) == 5001
    assert rows[-1]["Nome Fantasia"] == "Última Seguradora"


def test_utf8_bom_header_is_not_a_different_field(tmp_path: Path) -> None:
    path = tmp_path / "bom.csv"
    path.write_bytes(
        b"\xef\xbb\xbfNome Fantasia;Valor\n"
        + "Órbita;São Paulo\n".encode("utf-8")
    )
    assert detect_csv_encoding(path) == "utf-8-sig"
    assert _rows(path) == [{"Nome Fantasia": "Órbita", "Valor": "São Paulo"}]


def test_utf8_accents_without_bom(tmp_path: Path) -> None:
    path = tmp_path / "utf8.csv"
    path.write_text(
        "Nome Fantasia;Valor\nÓrbita;São Paulo\n",
        encoding="utf-8",
    )
    assert detect_csv_encoding(path) == "utf-8-sig"
    assert _rows(path) == [{"Nome Fantasia": "Órbita", "Valor": "São Paulo"}]


def test_legacy_latin1_accents(tmp_path: Path) -> None:
    path = tmp_path / "latin.csv"
    path.write_bytes(
        "Nome Fantasia;Valor\nÓrbita;São Paulo\n".encode("latin1")
    )
    assert detect_csv_encoding(path) == "latin1"
    assert _rows(path) == [{"Nome Fantasia": "Órbita", "Valor": "São Paulo"}]


def test_invalid_utf8_with_bom_fails_before_emitting_a_row(tmp_path: Path) -> None:
    path = tmp_path / "bad.csv"
    path.write_bytes(
        b"\xef\xbb\xbfNome Fantasia;Valor\nEmpresa;OK\nOutra;\xff\n"
    )
    reader = build._iter_rows(path)
    with pytest.raises(UnicodeDecodeError):
        next(reader)
    with pytest.raises(UnicodeDecodeError):
        direct._detect_csv_format(path)


def test_detection_and_reader_agree_on_late_latin1(tmp_path: Path) -> None:
    path = tmp_path / "legacy.csv"
    path.write_bytes(
        b"Nome Fantasia;Segmento de Mercado;Respondida;Situa\xe7\xe3o;"
        b"Avalia\xe7\xe3o Reclama\xe7\xe3o;Nota do Consumidor;"
        b"Tempo Resposta;Procurou Empresa;\xc1rea;Assunto;"
        b"Grupo Problema;Problema;Como Comprou Contratou;Data Finaliza\xe7\xe3o\n"
        + b"Empresa;Seguros;S;Finalizada;Resolvida;4;2;S;"
        b"X;X;X;X;X;03/06/2026\n"
    )
    encoding, delimiter, columns = direct._detect_csv_format(path)
    assert encoding == "latin1"
    assert delimiter == ";"
    assert columns[0] == "Nome Fantasia"
    assert len(_rows(path)) == 1


def test_aggregate_preserves_legacy_counts_without_replay(tmp_path: Path) -> None:
    path = tmp_path / "legacy.csv"
    header = (
        "Segmento de Mercado;Nome Fantasia;Situação;Respondida;"
        "Avaliação Reclamação;Nota do Consumidor\n"
    )
    row = (
        "Seguros, Capitalização e Previdência;Seguradora Exemplo;"
        "Finalizada;S;Resolvida;5\n"
    )
    path.write_bytes(
        header.encode("latin1")
        + row.encode("latin1") * 5001
        + (
            "Seguros, Capitalização e Previdência;Última Seguradora;"
            "Finalizada;S;Resolvida;5\n"
        ).encode("latin1")
    )
    result = build._aggregate_basecompleta(path, "2026-06", "https://example.test")
    assert result["meta"]["lines_total"] == 5002
    assert result["meta"]["lines_kept"] == 5002
    assert result["meta"]["companies"] == 2
    entries = result["by_name_key_raw"]
    assert sum(x["statistics"]["complaintsCount"] for x in entries.values()) == 5002
    assert sum(x["statistics"]["respondedCount"] for x in entries.values()) == 5002
    assert sum(x["statistics"]["scoreSum"] for x in entries.values()) == 25010.0


def test_late_decoding_error_after_multiple_chunks(tmp_path: Path) -> None:
    path = tmp_path / "multi_chunk.csv"
    path.write_bytes(
        b"Nome Fantasia;Valor\n"
        + b"Empresa;A\n" * 150000
        + "Última;B\n".encode("latin1")
    )
    assert detect_csv_encoding(path) == "latin1"
    counts = {"Empresa": 0, "Última": 0}
    for row in build._iter_rows(path):
        counts[row["Nome Fantasia"]] += 1
    assert counts == {"Empresa": 150000, "Última": 1}
