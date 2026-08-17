import zipfile
from pathlib import Path

from api.sources.receita_cnpj_bulk import (
    ReceitaOpenDataRelease,
    extract_target_lifecycle_from_zip,
    load_reason_map,
)


def _write_zip(path: Path, member: str, text: str) -> Path:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, text.encode("latin1"))
    return path


def test_load_reason_map_from_official_shape(tmp_path: Path):
    path = _write_zip(
        tmp_path / "Motivos.zip",
        "F.K03200$Z.D60319.MOTICCSV",
        '00;"SEM MOTIVO"\n02;"INCORPORACAO"\n',
    )

    reasons = load_reason_map(path)

    assert reasons["00"] == "SEM MOTIVO"
    assert reasons["02"] == "INCORPORACAO"


def test_extracts_only_exact_target_cnpj_and_lifecycle(tmp_path: Path):
    # Columns 0-7 follow the Receita Estabelecimentos layout used by lifecycle:
    # basic, order, DV, matriz/filial, fantasia, status, status date, reason.
    text = (
        '33061813;0001;40;1;"PRUDENTIAL";08;20241101;02\n'
        '21986074;0001;19;1;"PRUDENTIAL";02;20160310;00\n'
    )
    path = _write_zip(
        tmp_path / "Estabelecimentos0.zip",
        "K3241.K03200Y0.D60817.ESTABELE",
        text,
    )

    records = extract_target_lifecycle_from_zip(
        path,
        {"33061813000140": "PRUDENTIAL DO BRASIL SEGUROS DE VIDA S.A."},
        {"00": "SEM MOTIVO", "02": "INCORPORACAO"},
        source_url="https://example.gov/2026-08/Estabelecimentos0.zip",
        source_period="2026-08",
        observed_at="2026-08-17",
    )

    assert len(records) == 1
    row = records[0]
    assert row["cnpj"] == "33061813000140"
    assert row["cadastral_status"] == "closed"
    assert row["status_date"] == "2024-11-01"
    assert row["status_reason"] == "incorporation"
    assert row["source_mode"] == "official_open_data_bulk"
    assert row["source_period"] == "2026-08"
    assert row["raw_status_code"] == "08"
    assert row["raw_reason_code"] == "02"


def test_extract_supports_alphanumeric_cnpj(tmp_path: Path):
    text = '12ABC345;00DE;67;1;"EMPRESA TESTE";02;20260731;00\n'
    path = _write_zip(
        tmp_path / "Estabelecimentos1.zip",
        "ESTABELE",
        text,
    )

    records = extract_target_lifecycle_from_zip(
        path,
        {"12ABC34500DE67": "EMPRESA TESTE S.A."},
        {"00": "SEM MOTIVO"},
        source_url="https://example.gov/2026-08/Estabelecimentos1.zip",
        source_period="2026-08",
        observed_at="2026-08-17",
    )

    assert records[0]["cnpj"] == "12ABC34500DE67"
    assert records[0]["cadastral_status"] == "active"


def test_release_contract_carries_explicit_period_and_files():
    release = ReceitaOpenDataRelease(
        base_url="https://example.gov/",
        release_url="https://example.gov/2026-08/",
        period="2026-08",
        establishment_files=("Estabelecimentos0.zip", "Estabelecimentos1.zip"),
    )

    assert release.period == "2026-08"
    assert release.establishment_files[0] == "Estabelecimentos0.zip"
