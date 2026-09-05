import zipfile
from pathlib import Path

from api.sources.receita_cnpj_bulk import (
    ReceitaOpenDataRelease,
    _dav_resources,
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


def test_extract_accepts_zero_status_date_from_official_bulk(tmp_path: Path):
    text = '12345678;0001;90;1;"TESTE";02;0;00\n'
    path = _write_zip(tmp_path / "Estabelecimentos0.zip", "ESTABELE", text)
    records = extract_target_lifecycle_from_zip(
        path,
        {"12345678000190": "TESTE BULK S.A."},
        {"00": "SEM MOTIVO"},
        source_url="https://example.gov/2026-08/Estabelecimentos0.zip",
        source_period="2026-08",
        observed_at="2026-08-17",
    )

    assert records[0]["cadastral_status"] == "active"
    assert records[0]["status_date"] is None
    assert records[0]["data_quality_flags"] == ["missing_status_date"]


def test_extract_supports_alphanumeric_cnpj(tmp_path: Path):
    text = '12ABC345;00DE;67;1;"EMPRESA TESTE";02;20260731;00\n'
    path = _write_zip(tmp_path / "Estabelecimentos1.zip", "ESTABELE", text)
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


def test_parses_nextcloud_webdav_resource_metadata():
    xml = b'''<?xml version="1.0"?>
    <d:multistatus xmlns:d="DAV:">
      <d:response><d:href>/dav/2026-08/</d:href><d:propstat><d:prop>
        <d:displayname>2026-08</d:displayname>
        <d:getlastmodified>Sun, 09 Aug 2026 18:35:31 GMT</d:getlastmodified>
        <d:resourcetype><d:collection/></d:resourcetype>
      </d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>
      <d:response><d:href>/dav/2026-08/Estabelecimentos0.zip</d:href><d:propstat><d:prop>
        <d:displayname>Estabelecimentos0.zip</d:displayname>
        <d:getcontentlength>2200116910</d:getcontentlength>
        <d:getlastmodified>Sun, 09 Aug 2026 18:30:25 GMT</d:getlastmodified>
        <d:getetag>274eaa108d11a21301caf0346c0d76ec</d:getetag>
        <d:resourcetype/>
      </d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>
    </d:multistatus>'''
    rows = _dav_resources(xml)
    by_name = {row["name"]: row for row in rows}
    assert by_name["2026-08"]["is_collection"] is True
    assert by_name["Estabelecimentos0.zip"]["is_collection"] is False
    assert by_name["Estabelecimentos0.zip"]["size"] == 2200116910
    assert by_name["Estabelecimentos0.zip"]["etag"] == (
        "274eaa108d11a21301caf0346c0d76ec"
    )


def test_release_contract_carries_explicit_period_files_and_metadata():
    release = ReceitaOpenDataRelease(
        base_url="https://example.gov/share",
        release_url="https://example.gov/dav/2026-08/",
        period="2026-08",
        establishment_files=("Estabelecimentos0.zip", "Estabelecimentos1.zip"),
        resource_metadata=(
            {"name": "Estabelecimentos0.zip", "size": 2200116910},
        ),
    )
    assert release.period == "2026-08"
    assert release.establishment_files[0] == "Estabelecimentos0.zip"
    assert release.resource_metadata[0]["size"] == 2200116910
