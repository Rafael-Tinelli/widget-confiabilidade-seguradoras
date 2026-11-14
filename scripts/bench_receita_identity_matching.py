"""A09.3 bounded synthetic microbenchmark; NO official HTTP or full generation.

Run from repository root:
    python -m scripts.bench_receita_identity_matching --rows 30000 --repeats 4

Measures only the per-name CPU lookup hot path, not ZIP download, CSV parser,
national-row distribution, source completeness, Gate 4 or total runtime.
"""
from __future__ import annotations

import argparse
import json
import statistics
import time

from api.sources.receita_cnpj_identity import (
    _build_query_indexes,
    _business_core,
    _match_name_to_queries,
    _surface_name,
)


def _legacy_match(name: str, indexes: dict, *, prefix: str) -> list[tuple[str, str]]:
    if not name:
        return []
    out: list[tuple[str, str]] = []
    surface = _surface_name(name)
    for key in indexes["surface"].get(surface, set()):
        out.append((key, f"{prefix}_surface_exact"))
    core = _business_core(name)
    for key in indexes["core"].get(core, set()):
        out.append((key, f"{prefix}_business_core"))
    return out


def benchmark(rows: int, repeats: int) -> dict:
    if rows < 100 or repeats < 2:
        raise ValueError("rows must be >= 100 and repeats >= 2")
    # Synthetic only. Some names resemble the real matching questions, but
    # none of these rows is a Receita observation or used to build a registry.
    queries = _build_query_indexes([
        "PREVISUL", "Fairfax Brasil", "União Seguradora",
        "Equatorial Previdência", "Alpha Seguros",
    ])
    names = [
        f"{i % 1000000:06d} COMERCIO E SERVICOS DO BRASIL S A"
        for i in range(rows)
    ]
    names.extend([
        "PREVISUL", "UNIAO SEGURADORA S.A.",
        "FAIRFAX BRASIL SEGUROS CORPORATIVOS SA",
        "Equatorial Previdência Complementar", "Água & Fogo Seguros",
        "ALPHA SEGUROS LTDA",
    ] * max(1, rows // 300))
    for name in names:
        if _legacy_match(name, queries, prefix="legal_name") != _match_name_to_queries(
            name, queries, prefix="legal_name",
        ):
            raise AssertionError("Benchmark output parity failure")

    def timed(fn):
        started = time.perf_counter()
        count = 0
        for name in names:
            count += len(fn(name, queries, prefix="legal_name"))
        return time.perf_counter() - started, count

    # Alternate order to limit warm-up/clock bias; no performance threshold.
    legacy_times, optimized_times = [], []
    for i in range(repeats):
        if i % 2 == 0:
            old, count_old = timed(_legacy_match)
            new, count_new = timed(_match_name_to_queries)
        else:
            new, count_new = timed(_match_name_to_queries)
            old, count_old = timed(_legacy_match)
        assert count_old == count_new
        legacy_times.append(old)
        optimized_times.append(new)
    old_median = statistics.median(legacy_times)
    new_median = statistics.median(optimized_times)
    return {
        "benchmark": "a09_3_receita_name_matching_synthetic_cpu_only",
        "sample": "synthetic_not_official",
        "rows": len(names),
        "repeats": repeats,
        "legacy_median_seconds": round(old_median, 6),
        "optimized_median_seconds": round(new_median, 6),
        "name_matching_time_reduction_pct": round(
            100 * (1 - new_median / old_median), 2
        ),
        "semantic_parity": True,
        "full_generation_time_reduction": "not_measured",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=30_000)
    parser.add_argument("--repeats", type=int, default=4)
    args = parser.parse_args()
    print(json.dumps(benchmark(args.rows, args.repeats), indent=2))


if __name__ == "__main__":
    main()
