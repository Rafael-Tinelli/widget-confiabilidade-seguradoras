"""A09.3: semantic equivalence of optimized Receita name matching.

No official downloads, full generation, or changes in admitted identities.
The legacy routine is frozen here as a regression oracle.
"""
from __future__ import annotations

import io
import json
import random
import zipfile
from pathlib import Path

from api.sources import receita_cnpj_bulk as bulk
from api.sources import receita_cnpj_identity as identity


def legacy_match(name: str, indexes: dict, *, prefix: str) -> list[tuple[str, str]]:
    if not name:
        return []
    out: list[tuple[str, str]] = []
    surface = identity._surface_name(name)
    for key in indexes["surface"].get(surface, set()):
        out.append((key, f"{prefix}_surface_exact"))
    core = identity._business_core(name)
    for key in indexes["core"].get(core, set()):
        out.append((key, f"{prefix}_business_core"))
    return out


def test_a093_matching_preserves_exact_methods_order_and_ambiguity() -> None:
    indexes = identity._build_query_indexes([
        "Previsul", "União Seguradora", "Fairfax Brasil",
        "Equatorial Previdência", "Alpha de Seguros", "Capital S.A.",
    ])
    known = [
        "", "PREVISUL", "União Seguradora S.A.",
        "FAIRFAX BRASIL SEGUROS CORPORATIVOS SA",
        "Equatorial Previdência Complementar",
        "Alpha de Seguros LTDA", "Capital S.A.",
        "ACME S A", "S.A. SA LTDA", "ÁGUA & FOGO S/A",
        "ÁÁÁÅ cïà", "0000", "- - -", "UM", "X", "SA",
        "União Seguradora S.A S.A", "PREVISUL,  Ltda.",
        "MUNICIPAL DE SERVIÇOS DE SEGUROS",
    ]
    rng = random.Random(9023)
    tokens = [
        "Companhia", "Seguradora", "SEGUROS", "LTDA", "S.A.",
        "Brasil", "Previsul", "Fairfax", "Água", "Equatorial",
        "Previdência", "União", "0", "0012", "Sociedade",
        "Financeira", "Corp.", "--", "  ",
    ]
    names = known + [
        " ".join(rng.choice(tokens) for _ in range(rng.randrange(1, 8)))
        for _ in range(4_000)
    ]
    for prefix in ("trade_name", "legal_name"):
        for name in names:
            assert identity._match_name_to_queries(
                name, indexes, prefix=prefix,
            ) == legacy_match(name, indexes, prefix=prefix)


def test_a093_one_normalization_per_name(monkeypatch) -> None:
    indexes = identity._build_query_indexes(["Previsul", "Fairfax Brasil"])
    original = identity.normalize_name_key
    calls: list[str] = []

    def counted(name: str) -> str:
        calls.append(name)
        return original(name)

    monkeypatch.setattr(identity, "normalize_name_key", counted)
    got = identity._match_name_to_queries(
        "FAIRFAX BRASIL SEGUROS CORPORATIVOS SA",
        indexes, prefix="legal_name",
    )
    assert got == [("fairfax brasil", "legal_name_business_core")]
    assert calls == ["FAIRFAX BRASIL SEGUROS CORPORATIVOS SA"]


def _zip(data: str) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("dados.csv", data.encode("latin1"))
    return output.getvalue()


def _establishment(
    base: str, branch: str, digits: str, *,
    trade: str, head: str = "1",
) -> str:
    return (
        f"{base};{branch};{digits};{head};{trade};02;20260110"
        ";;;;20010101;6511101;6622300"
    )


def test_a093_full_filtered_identity_parity_on_local_multi_partition_zip(
    monkeypatch, tmp_path: Path,
) -> None:
    payloads = {
        "Cnaes.zip": _zip("6511101;SEGURADORA\n6622300;AGENTE\n"),
        "Naturezas.zip": _zip("2054;SOCIEDADE ANONIMA\n"),
        "Empresas0.zip": _zip(
            "92751213;PREVISUL SEGURADORA SA;2054\n"
            "11111111;FAIRFAX BRASIL SEGUROS CORPORATIVOS SA;2054\n"
            "22222222;EMPRESA DESCARTADA 222;9999\n"
        ),
        "Empresas1.zip": _zip(
            "33333333;OUTRA EMPRESA COMERCIO LTDA;2054\n"
            "44444444;UNIAO SEGURADORA SA;2054\n"
        ),
        "Estabelecimentos0.zip": _zip(
            _establishment("92751213", "0001", "73", trade="PREVISUL")
            + "\n" + _establishment(
                "11111111", "0001", "11", trade="FAIRFAX BRASIL"
            ) + "\n" + _establishment(
                "22222222", "0001", "22", trade="LOJA DE FERRAGENS"
            ) + "\n"
        ),
        "Estabelecimentos1.zip": _zip(
            _establishment("33333333", "0001", "33", trade="OUTROS SERVICOS")
            + "\n" + _establishment(
                "44444444", "0001", "44", trade="União Seguradora SA"
            ) + "\n"
        ),
    }
    metadata = {
        name: {
            "name": name, "size": len(data),
            "etag": f"etag-{i}", "last_modified": "2026-11-01",
        }
        for i, (name, data) in enumerate(payloads.items())
    }
    release = bulk.ReceitaOpenDataRelease(
        base_url="https://www.gov.br/",
        release_url="https://arquivos.receitafederal.gov.br/2026-10/",
        period="2026-10",
        establishment_files=("Estabelecimentos0.zip", "Estabelecimentos1.zip"),
    )
    monkeypatch.setattr(identity, "_release_files", lambda *_a, **_k: {
        "companies": ["Empresas0.zip", "Empresas1.zip"],
        "cnaes": "Cnaes.zip", "legal_natures": "Naturezas.zip",
        "metadata": metadata,
    })

    def download(_session, _url, destination: Path) -> Path:
        destination.write_bytes(payloads[destination.name])
        return destination

    monkeypatch.setattr(identity, "_download_zip", download)
    targets = [
        {"cnpj": "92751213000173", "entity_id": "target-previsul",
         "legal_name": "PREVISUL SEGURADORA SA"},
        {"cnpj": "33333333000133", "entity_id": "target-other",
         "legal_name": "OUTRA EMPRESA COMERCIO LTDA"},
    ]
    queries = ["PREVISUL", "Fairfax Brasil", "União Seguradora"]
    optimized = identity._match_name_to_queries

    monkeypatch.setattr(identity, "_match_name_to_queries", legacy_match)
    previous = identity.build_filtered_identity_snapshot(
        targets, queries, release=release, work_dir=tmp_path / "old",
    )
    monkeypatch.setattr(identity, "_match_name_to_queries", optimized)
    current = identity.build_filtered_identity_snapshot(
        targets, queries, release=release, work_dir=tmp_path / "new",
    )

    # Exclude only observation-clock timestamps; all selection, ranking,
    # legal nature, CNAE, source identity and partition coverage must agree.
    for result in (previous, current):
        result.pop("generated_at")
        result["source"].pop("retrieved_at")
        result["source"].pop("release_checked_at")
    assert json.dumps(previous, sort_keys=True) == json.dumps(
        current, sort_keys=True,
    )
    assert len(current["canonical_records"]) == 2
    assert len(current["provider_matches"]) == 3
    assert current["meta"]["company_files_scanned"] == [
        "Empresas0.zip", "Empresas1.zip",
    ]
    assert current["meta"]["establishment_files_scanned"] == [
        "Estabelecimentos0.zip", "Estabelecimentos1.zip",
    ]
    assert sum(
        row["candidate_state"] == "unique_candidate"
        for row in current["provider_matches"]
    ) == 3
