from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from api.sources.susep_insurance_exposure import (
    InsuranceExposureSourceError,
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
    assert probe["currency"] == "BRL"
    assert probe["source_unit_label"] == "R$"
    assert probe["scale_factor_applied"] == 1.0
    assert probe["source_documentation_url"].endswith("Documentacao_das_tabelas.rtf")
    assert probe["explicitly_excluded_domains"] == ["private_pension", "capitalization"]

    payload = load_susep_insurance_exposure(["000001"], path)
    assert payload["source"]["component_file"] == "Ses_seguros.csv"
    assert payload["source"]["currency"] == "BRL"
    assert payload["source"]["source_unit_label"] == "R$"
    assert payload["source"]["scale_factor_applied"] == 1.0
    assert payload["source"]["unit_policy"] == "raw_ses_currency_values_no_scale_conversion"
    assert payload["source"]["missingness_policy"] == (
        "missing_premium_cells_are_not_economic_zero"
    )
    assert payload["source"]["malformed_value_policy"] == "fail_closed_not_missing"
    assert payload["source"]["malformed_row_policy"] == "fail_closed_not_skipped"
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
        "insurance_premium_direct_missing_rows",
        "insurance_premium_earned_missing_rows",
        "insurance_branches",
    }
    assert jan["insurance_premium_direct"] == pytest.approx(150.0)
    assert jan["insurance_premium_earned"] == pytest.approx(135.0)
    assert jan["insurance_premium_direct_missing_rows"] == 0
    assert jan["insurance_premium_earned_missing_rows"] == 0


def test_missing_direct_premium_is_preserved_not_imputed_as_zero(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    insurance = (
        "damesano;coenti;coramo;premio_direto;premio_ganho\n"
        "202601;1;1001;;90,00\n"
        "202601;1;2001;50,00;45,00\n"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("Ses_seguros.csv", insurance.encode("latin1"))

    payload = load_susep_insurance_exposure(["000001"], path)
    jan = payload["entities"]["000001"]["months"][202601]

    assert jan["insurance_premium_direct"] == pytest.approx(50.0)
    assert jan["insurance_premium_direct_missing_rows"] == 1
    assert jan["insurance_branches"][1001]["premium_direct_missing_rows"] == 1.0
    assert jan["insurance_branches"][1001]["premium_direct"] == 0.0


def test_row_with_both_premiums_missing_is_preserved_as_missingness(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    insurance = (
        "damesano;coenti;coramo;premio_direto;premio_ganho\n"
        "202601;1;1001;;\n"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("Ses_seguros.csv", insurance.encode("latin1"))

    payload = load_susep_insurance_exposure(["000001"], path)
    jan = payload["entities"]["000001"]["months"][202601]

    assert jan["insurance_premium_direct"] == 0.0
    assert jan["insurance_premium_earned"] == 0.0
    assert jan["insurance_premium_direct_missing_rows"] == 1
    assert jan["insurance_premium_earned_missing_rows"] == 1
    assert jan["insurance_branches"][1001]["rows"] == 1.0


def test_parser_nan_premium_cell_is_preserved_as_missingness(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    insurance = (
        "damesano;coenti;coramo;premio_direto;premio_ganho\n"
        "202601;1;1001;NaN;90,00\n"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("Ses_seguros.csv", insurance.encode("latin1"))

    payload = load_susep_insurance_exposure(["000001"], path)
    jan = payload["entities"]["000001"]["months"][202601]
    assert jan["insurance_premium_direct_missing_rows"] == 1
    assert jan["insurance_premium_direct"] == 0.0


def test_malformed_premium_cell_fails_closed_instead_of_becoming_missing(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    insurance = (
        "damesano;coenti;coramo;premio_direto;premio_ganho\n"
        "202601;1;1001;not-a-number;90,00\n"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("Ses_seguros.csv", insurance.encode("latin1"))

    with pytest.raises(InsuranceExposureSourceError, match="premio_direto"):
        load_susep_insurance_exposure(["000001"], path)


def test_valid_historical_aaaamm_period_is_preserved(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    insurance = (
        "damesano;coenti;coramo;premio_direto;premio_ganho\n"
        "199501;1;1001;100,00;90,00\n"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("Ses_seguros.csv", insurance.encode("latin1"))

    payload = load_susep_insurance_exposure(["000001"], path)

    assert payload["periods"] == [199501]
    assert payload["reference_period"] == 199501
    assert 199501 in payload["entities"]["000001"]["months"]


def test_unclassified_historical_zero_premium_row_is_ignored_and_audited(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BaseCompleta.zip"
    insurance = (
        "damesano;coenti;coramo;premio_direto;premio_ganho\n"
        "199503;5321;;0;0\n"
        "199503;5321;0111;100,00;90,00\n"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("Ses_seguros.csv", insurance.encode("latin1"))

    payload = load_susep_insurance_exposure(["005321"], path)
    month = payload["entities"]["005321"]["months"][199503]

    assert month["insurance_premium_direct"] == pytest.approx(100.0)
    assert month["insurance_premium_earned"] == pytest.approx(90.0)
    assert set(month["insurance_branches"]) == {111}
    assert payload["source"]["ignored_unclassified_zero_premium_rows"] == 1
    assert payload["source"]["unclassified_branch_policy"] == (
        "ignore_only_when_direct_and_earned_premiums_are_explicit_zero;"
        "otherwise_fail_closed"
    )


@pytest.mark.parametrize(
    ("direct", "earned"),
    [("1", "0"), ("0", "1"), ("", "0"), ("0", "")],
)
def test_missing_branch_with_value_or_missingness_fails_closed(
    tmp_path: Path,
    direct: str,
    earned: str,
) -> None:
    path = tmp_path / "BaseCompleta.zip"
    insurance = (
        "damesano;coenti;coramo;premio_direto;premio_ganho\n"
        f"199503;5321;;{direct};{earned}\n"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("Ses_seguros.csv", insurance.encode("latin1"))

    with pytest.raises(InsuranceExposureSourceError, match="missing coramo"):
        load_susep_insurance_exposure(["005321"], path)


def test_malformed_period_or_branch_fails_closed(tmp_path: Path) -> None:
    for row, field in (
        ("202613;1;1001;100,00;90,00\n", "damesano"),
        ("202601;1;bad;100,00;90,00\n", "coramo"),
    ):
        path = tmp_path / f"BaseCompleta-{field}.zip"
        insurance = (
            "damesano;coenti;coramo;premio_direto;premio_ganho\n" + row
        )
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr("Ses_seguros.csv", insurance.encode("latin1"))

        with pytest.raises(InsuranceExposureSourceError, match=field):
            load_susep_insurance_exposure(["000001"], path)


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
