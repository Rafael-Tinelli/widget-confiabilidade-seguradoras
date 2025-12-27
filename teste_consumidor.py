from __future__ import annotations

import json
import os

from api.sources.consumidor_gov import (
    aggregate_month,
    discover_basecompleta_urls,
    download_csv_to_gz,
)


def test() -> None:
    urls = discover_basecompleta_urls(months=1)
    if not urls:
        raise SystemExit("Nenhuma URL encontrada para Base Completa.")

    ym, url = list(urls.items())[0]
    print(f"Testando mês: {ym} -> {url}")

    out_dir = "data/snapshots/_tmp"
    os.makedirs(out_dir, exist_ok=True)
    gz_path = os.path.join(out_dir, f"test_consumidor_{ym}.csv.gz")

    download_csv_to_gz(url, gz_path)

    aggs = aggregate_month(gz_path)

    top5 = sorted(aggs.values(), key=lambda x: x.total, reverse=True)[:5]
    print("\n--- TOP 5 EMPRESAS (TESTE) ---")
    for a in top5:
        print(json.dumps(a.to_public(), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    test()
