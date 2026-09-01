from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

import api.sources.susep_financial_evidence as financial_source
from api.sources.susep_financial_evidence import (
    FinancialEvidenceSourceError,
    load_susep_financial_evidence,
)


def _write_zip(
    path: Path,
    *,
    june_new_pla: str = "950,25",
    balance_value: str = "1000",
    june_premium: str = "20",
    capital_fip: str = "1",
    capital_period: str = "202606",
    balance_period: str = "202606",
    balance_cmpid: str = "1479",
    premium_period: str = "202606",
    premium_rows: str | None = None,
    extra_capital_rows: str = "",
) -> None:
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(
            "Ses_pl_margem.csv",
            "coenti;damesano;plajustado;margem;pl;AjustesContabeis;"
            "AjustesEconomicos;NovoPla;CMR\n"
            "1;202605;1.234,56;100;1200;0;0;1.345,67;900\n"
            f"{capital_fip};{capital_period};-100,50;100;1200;0;"
            "-3,72529029846191E-9;"
            f"{june_new_pla};800\n"
            f"{extra_capital_rows}"
            "2;202606;;100;1200;0;0;;700\n",
        )
        z.writestr(
            "SES_Balanco.csv",
            "coenti;damesano;cmpid;valor;seq;quadro\n"
            f"1;{balance_period};{balance_cmpid};{balance_value};1;22A\n"
            "1;202606;11160;10;2;22A\n"
            "1;202606;351;5;3;22A\n"
            "1;202606;1040;500;4;22P\n"
            "1;202606;331;100;5;22A\n"
            "1;202606;11187;0;6;22A\n"
            "1;202606;5503;0;7;22A\n"
            "1;202606;6449;50;8;22P\n"
            "2;202606;1479;100;1;22A\n"
            "2;202606;1040;50;2;22P\n",
        )
        z.writestr(
            "Ses_seguros.csv",
            "damesano;coenti;premio_ganho\n"
            + (
                premium_rows
                if premium_rows is not None
                else (
                    "202605;1;10\n"
                    f"{premium_period};1;{june_premium}\n"
                    "202606;2;0\n"
                )
            ),
        )


def test_reader_preserves_missing_negative_and_formula_components(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path)

    payload = load_susep_financial_evidence(["000001", "000002"], path)

    assert payload["source"]["malformed_row_policy"] == "fail_closed_not_skipped"
    assert payload["source"]["key_parsing_policy"] == (
        "strict_integer_keys_and_valid_aaaamm_periods"
    )
    assert payload["source"]["numeric_parsing_policy"] == (
        "strict_finite_decimal_or_scientific_notation"
    )
    assert payload["reference_periods"] == {
        "capital": 202606,
        "balance": 202606,
        "insurance_operations": 202606,
    }
    one = payload["entities"]["000001"]
    assert one["capital_history"][202605]["pla_adjusted"] == 1234.56
    assert one["capital_history"][202605]["new_pla"] == 1345.67
    assert one["capital_history"][202606]["pla_adjusted"] == -100.5
    assert one["capital_history"][202606]["new_pla"] == 950.25
    assert one["capital_history"][202606]["cmr"] == 800.0
    assert one["capital_history"][202606]["economic_adjustments"] == pytest.approx(
        -3.72529029846191e-9
    )
    assert set(one["balance_values"][202606]) >= {1479, 11160, 351, 1040}
    assert one["nonzero_premium_periods"] == {202605, 202606}

    two = payload["entities"]["000002"]
    assert two["capital_history"][202606]["pla_adjusted"] is None
    assert two["capital_history"][202606]["new_pla"] is None
    assert two["capital_history"][202606]["cmr"] == 700.0
    assert two["nonzero_premium_periods"] == set()


@pytest.mark.parametrize("june_new_pla", ["1abc", "1e309"])
def test_reader_rejects_malformed_or_nonfinite_financial_number(
    tmp_path: Path,
    june_new_pla: str,
) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path, june_new_pla=june_new_pla)

    with pytest.raises(FinancialEvidenceSourceError, match="novopla"):
        load_susep_financial_evidence(["000001"], path)


@pytest.mark.parametrize(
    ("source_override", "field"),
    [
        ({"balance_value": "1abc"}, "valor"),
        ({"june_premium": "1abc"}, "premio_ganho"),
    ],
)
def test_reader_rejects_malformed_balance_or_operation_number(
    tmp_path: Path,
    source_override: dict[str, str],
    field: str,
) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path, **source_override)

    with pytest.raises(FinancialEvidenceSourceError, match=field):
        load_susep_financial_evidence(["000001"], path)


@pytest.mark.parametrize(
    ("source_override", "field"),
    [
        ({"capital_fip": "1abc"}, "coenti"),
        ({"capital_period": "202606.5"}, "damesano"),
        ({"premium_period": "202613"}, "damesano"),
        ({"balance_cmpid": "1479.5"}, "cmpid"),
    ],
)
def test_reader_rejects_malformed_or_fractional_source_keys(
    tmp_path: Path,
    source_override: dict[str, str],
    field: str,
) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path, **source_override)

    with pytest.raises(FinancialEvidenceSourceError, match=field):
        load_susep_financial_evidence(["000001"], path)


def test_reader_rejects_malformed_requested_fip_instead_of_stripping_it(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path)

    with pytest.raises(FinancialEvidenceSourceError, match="requested FIP"):
        load_susep_financial_evidence(["abc000001"], path)


def test_reader_tracks_duplicate_capital_rows_by_period(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(
        path,
        extra_capital_rows="1;202606;100;100;1200;0;0;960;800\n",
    )

    payload = load_susep_financial_evidence(["000001"], path)
    entity = payload["entities"]["000001"]

    assert entity["duplicate_capital_rows"] == 1
    assert entity["duplicate_capital_rows_by_period"] == {202606: 1}


def test_nonzero_operation_presence_is_aggregated_across_csv_chunks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(
        path,
        premium_rows=(
            "202606;1;20\n"
            "202606;1;-20\n"
        ),
    )
    monkeypatch.setattr(financial_source, "SES_CSV_CHUNK_ROWS", 1)

    payload = load_susep_financial_evidence(["000001"], path)

    assert payload["entities"]["000001"]["insurance_operation_periods"] == {
        202606
    }
    assert payload["entities"]["000001"]["nonzero_premium_periods"] == set()


def test_malformed_csv_row_fails_closed_instead_of_being_skipped(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(
            "Ses_pl_margem.csv",
            "coenti;damesano;plajustado;margem;pl;AjustesContabeis;"
            "AjustesEconomicos;NovoPla;CMR\n"
            "1;202605;100;100;1200;0;0;950;800\n"
            "1;202606;100;100;1200;0;0;950;800;unexpected\n",
        )
        z.writestr(
            "SES_Balanco.csv",
            "coenti;damesano;cmpid;valor\n1;202606;1479;1000\n",
        )
        z.writestr(
            "Ses_seguros.csv",
            "damesano;coenti;premio_ganho\n202606;1;20\n",
        )

    with pytest.raises(FinancialEvidenceSourceError, match="unable to parse"):
        load_susep_financial_evidence(["000001"], path)
