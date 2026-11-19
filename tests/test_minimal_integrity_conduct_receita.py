"""Regression cases from second independent audit; local data only.

No real source acquisition, Gate 4, HostGator or production mutation.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from api import build_consumidor_gov as build
from api.sources import consumer_gov_direct as direct
from api.sources import receita_cnpj_identity as identity
from api.sources.receita_cnpj_bulk import ReceitaOpenDataError
from api.v2.consumer_gov_window import ConsumerGovWindowError, load_active_window

MONTH = "2026-03"


def _csv(path: Path, *, variant: bool, rows: int = 1, score: str = "4") -> None:
    headers = {field: aliases[0] for field, aliases in direct.REQUIRED_FIELDS.items()}
    if variant:
        headers.update({
            "responded": "respondida", "situation": "Situacao",
            "evaluation": "Avaliacao Reclamacao", "score": "nota do consumidor",
        })
    row = {
        headers["provider"]: "Exemplo Seguradora",
        headers["segment"]: build.TARGET_SEGMENT,
        headers["responded"]: "S",
        headers["situation"]: "Finalizada Avaliada",
        headers["evaluation"]: "Resolvida",
        headers["score"]: score,
        headers["response_time"]: "3",
        headers["contacted_company"]: "S",
        headers["area"]: "Servicos Financeiros",
        headers["subject"]: "Seguro",
        headers["problem_group"]: "Atendimento",
        headers["problem"]: "Problema" + ("X" * 21_000 if rows >= 960 else ""),
        headers["purchase_channel"]: "Internet",
        "Data Abertura": "03/03/2026",
    }
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row), delimiter=";")
        writer.writeheader()
        writer.writerows([row] * rows)


@pytest.mark.parametrize("rows", [1, 960])
def test_validated_header_aliases_produce_identical_conduta_counters(
    tmp_path: Path, rows: int,
) -> None:
    snapshots = []
    for variant in (False, True):
        source = tmp_path / f"{'variant' if variant else 'canonical'}-{rows}.csv"
        _csv(source, variant=variant, rows=rows)
        check = direct.validate_month_csv(
            source, MONTH, min_bytes=1 if rows == 1 else None,
        )
        root = build._aggregate_basecompleta(
            source, MONTH, "https://dados.mj.gov.br/fixture",
            field_map=check.field_map,
        )
        stats = next(iter(root["by_name_key_raw"].values()))["statistics"]
        assert check.rows == root["meta"]["lines_total"] == rows
        assert root["meta"]["lines_kept"] == rows
        assert stats["complaintsCount"] == rows
        assert stats["respondedCount"] == rows
        assert stats["finalizedCount"] == rows
        assert stats["resolvedCount"] == rows
        assert stats["satisfactionCount"] == rows
        assert stats["scoreSum"] == 4.0 * rows
        snapshots.append(stats)
    assert snapshots[0] == snapshots[1]


def test_direct_aggregator_resolves_alias_map_even_without_explicit_argument(
    tmp_path: Path,
) -> None:
    source = tmp_path / "aliases.csv"
    _csv(source, variant=True)
    stats = next(iter(
        build._aggregate_basecompleta(
            source, MONTH, "https://dados.mj.gov.br/fixture",
        )["by_name_key_raw"].values()
    ))["statistics"]
    assert (stats["respondedCount"], stats["resolvedCount"], stats["scoreSum"]) == (
        1, 1, 4.0,
    )


@pytest.mark.parametrize("score", ["3,5", "0", "6", "nan"])
def test_fractional_or_invalid_csv_score_rejected_before_aggregation(
    tmp_path: Path, score: str,
) -> None:
    source = tmp_path / "bad.csv"
    _csv(source, variant=True, score=score)
    with pytest.raises(direct.ConsumerGovSchemaMismatch):
        direct.validate_month_csv(source, MONTH, min_bytes=1)
    with pytest.raises(ValueError, match="score"):
        build._aggregate_basecompleta(
            source, MONTH, "https://dados.mj.gov.br/fixture",
        )


@pytest.mark.parametrize("score,note_count", [(1.0, 2), (7.5, 2), (0.0, 1)])
def test_a08_rejects_impossible_rating_sums_with_consistent_mean(
    tmp_path: Path, score: float, note_count: int,
) -> None:
    # Use the fully declared historical-shaped test factory already present
    # in A08, so the failure isolates the domain of the rating scale.
    from test_consumer_gov_a08_integrity import _legacy_window

    def corrupt(roots, agg):
        for root in [*roots, agg]:
            stats = root["by_name_key_raw"]["seguradora demonstracao"]["statistics"]
            multiplier = len(roots) if root is agg else 1
            stats["satisfactionCount"] = note_count * multiplier
            stats["scoreSum"] = score * multiplier
            stats["averageScore"] = round(score / note_count, 4)

    derived, monthly = _legacy_window(tmp_path, corrupt=corrupt)
    with pytest.raises(ConsumerGovWindowError, match="integer 1..5 ratings"):
        load_active_window(derived, monthly, required=3)


def _mock_catalog(company_names: list[str]) -> list[dict]:
    names = [*company_names, "Cnaes.zip", "Naturezas.zip"]
    return [
        {
            "name": name, "size": 1024, "etag": '"official"',
            "is_collection": False,
        } for name in names
    ]


@pytest.mark.parametrize("companies", [
    ["Empresas0.zip", "Empresas2.zip"],
    ["Empresas1.zip", "Empresas2.zip"],
    ["Empresas0.zip", "Empresas00.zip"],
    ["Empresas0.zip", "Empresas1.zip", "Empresas3.zip"],
])
def test_receita_incomplete_company_family_rejected_before_fingerprint_or_get(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, companies: list[str],
) -> None:
    monkeypatch.setattr(
        identity, "_propfind", lambda _session, _url: _mock_catalog(companies),
    )
    monkeypatch.setattr(
        identity, "_download_zip",
        lambda *_a, **_kw: pytest.fail("no GET before company inventory validation"),
    )
    monkeypatch.setattr(identity, "_configure_official_session", lambda _s: None)
    release = SimpleNamespace(
        period="2026-11",
        release_url="https://arquivos.receitafederal.gov.br/public.php/dav/files/token/2026-11/",
        establishment_files=("Estabelecimentos0.zip",),
    )
    with pytest.raises(ReceitaOpenDataError, match="partition coverage"):
        identity._release_files(None, release)
    with pytest.raises(ReceitaOpenDataError, match="partition coverage"):
        identity.build_filtered_identity_snapshot(
            [], ["Exemplo Patrimonial"], release=release, session=SimpleNamespace(),
            work_dir=tmp_path,
        )
    assert not any(tmp_path.iterdir())


@pytest.mark.parametrize("n", [1, 3, 11])
def test_receita_complete_0_to_n_minus_1_accepted_without_fixed_count(
    monkeypatch: pytest.MonkeyPatch, n: int,
) -> None:
    names = [f"Empresas{i}.zip" for i in reversed(range(n))]
    monkeypatch.setattr(identity, "_propfind", lambda _a, _b: _mock_catalog(names))
    result = identity._release_files(
        None, SimpleNamespace(release_url="https://arquivos.receitafederal.gov.br/x/")
    )
    assert result["companies"] == [f"Empresas{i}.zip" for i in range(n)]
