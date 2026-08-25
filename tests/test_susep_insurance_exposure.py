from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from api.sources.susep_insurance_exposure import (
    load_susep_insurance_exposure,
    probe_susep_insurance_exposure,
)


def _write_insurance_only_zip(path: Path) -> None:
    insurance = (
        "damesano;coenti;coramo;premio_direto;premio_ganho\n"
        "202601;1;1001;100,00;90,00\n"
        "202601;1;2001;50,00;45,00\n"
        "202602;1;1001;200,00;180,00\n"
        "202601;2;1001;999,00;900,00\n"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("Ses_seguros.csv", insurance.encode("latin1"))


def test_insurance_exposure_does_not_require_pension_or_capitalization_files(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_insurance_only_zip(path)

    probe = probe_susep_insurance_exposure(path)
    assert probe["state"] == "available"
    assert probe["exposure_domain"] == "insurance_only"
    assert probe["explicitly_excluded_domains"] == ["private_pension", "capitalization"]

    payload = load_susep_insurance_exposure(["000001"], path)
    assert payload["source"]["component_file"] == "Ses_seguros.csv"
    assert payload["source"]["explicitly_excluded_domains"] == [
        "private_pension",
        "capitalization",
    ]
    assert "pension_component_file" not in payload["source"]
    assert "capitalization_component_file" not in payload["source"]

    jan = payload["entities"]["000001"]["months"][202601]
    assert set(jan) == {
        "insurance_premium_direct",
        "insurance_premium_earned",
        "insurance_branches",
    }
    assert jan["insurance_premium_direct"] == pytest.approx(150.0)
    assert jan["insurance_premium_earned"] == pytest.approx(135.0)


def test_insurance_exposure_filters_requested_fips(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_insurance_only_zip(path)
    payload = load_susep_insurance_exposure(["000001"], path)
    assert set(payload["entities"]) == {"000001"}


def test_probe_fails_closed_when_insurance_schema_is_incomplete(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(
            "Ses_seguros.csv",
            "damesano;coenti;premio_direto;premio_ganho\n202601;1;10;9\n".encode(
                "latin1"
            ),
        )
    probe = probe_susep_insurance_exposure(path)
    assert probe["state"] == "schema_incompatible"
    assert probe["missing_columns"] == ["coramo"]
