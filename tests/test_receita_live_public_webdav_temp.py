from __future__ import annotations

from api.sources.receita_cnpj_bulk import discover_latest_release


def test_receita_live_public_webdav_probe() -> None:
    release = discover_latest_release()
    assert release.period.startswith("20")
    assert release.establishment_files
    assert release.reasons_file == "Motivos.zip"
