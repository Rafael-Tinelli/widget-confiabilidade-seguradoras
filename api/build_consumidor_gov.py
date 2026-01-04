# api/build_consumidor_gov.py
from __future__ import annotations

import json
import os  # Fixed: Added missing import
import sys
from dataclasses import asdict
from datetime import datetime

# Importa do novo motor (com nome correto)
from api.sources.consumidor_gov import Agg, sync_monthly_cache_from_dump_if_needed, _utc_now

DERIVED_DIR = "data/derived/consumidor_gov"
MONTHLY_DIR = f"{DERIVED_DIR}/monthly"
OUT_LATEST = f"{DERIVED_DIR}/aggregated.json"


def _load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _merge_raw_into(dst: dict[str, Agg], key: str, raw: dict) -> None:
    a = dst.get(key)
    if not a:
        a = Agg(display_name=str(raw.get("display_name") or ""))
        dst[key] = a
    a.merge_raw(raw)


def main(months: int = 12) -> None:
    print("\n--- BUILD CONSUMIDOR.GOV (HYBRID ORCHESTRATOR) ---")

    # 1. Definir Janela de Tempo (Últimos X meses)
    today = datetime.now()
    target_yms = []
    for i in range(months):
        y = today.year
        m = today.month - i
        while m <= 0:
            m += 12
            y -= 1
        target_yms.append(f"{y:04d}-{m:02d}")

    print(f"CG: Janela alvo: {target_yms}")

    # 2. Sync: Baixa dump e processa (se faltar mês)
    sync_monthly_cache_from_dump_if_needed(target_yms, MONTHLY_DIR)

    # 3. Merge: Consolida meses disponíveis
    merged_name: dict[str, Agg] = {}
    merged_cnpj: dict[str, Agg] = {}
    months_found = []

    for ym in target_yms:
        p = os.path.join(MONTHLY_DIR, f"consumidor_gov_{ym}.json")
        if not os.path.exists(p):
            continue

        try:
            month_data = _load_json(p)
            months_found.append(ym)

            raw_n = month_data.get("by_name_key_raw", {})
            raw_c = month_data.get("by_cnpj_key_raw", {})

            for k, v in raw_n.items():
                _merge_raw_into(merged_name, k, v)
            for k, v in raw_c.items():
                _merge_raw_into(merged_cnpj, k, v)

        except Exception as e:
            print(f"CG: Erro lendo {p}: {e}")

    # 4. Guard Rail: Evita zerar o site se o download falhar
    if not merged_name:
        print("CG: FATAL - Nenhum dado consolidado. Abortando.")
        sys.exit(1)

    # 5. Exportação
    out = {
        "meta": {
            "generated_at": _utc_now(),
            "window_months": len(months_found),
            "months": months_found,
            "source": "consumidor.gov.br (Dump)",
            "stats": {
                "total_companies": len(merged_name),
                "companies_with_cnpj": len(merged_cnpj)
            }
        },
        "by_name": {k: v.to_public() for k, v in merged_name.items()},
        "by_cnpj_key": {k: v.to_public() for k, v in merged_cnpj.items()},
        "by_name_key_raw": {k: asdict(v) for k, v in merged_name.items()}
    }

    os.makedirs(os.path.dirname(OUT_LATEST), exist_ok=True)
    with open(OUT_LATEST, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(',', ':'))

    print(f"OK: Agregado salvo em {OUT_LATEST}")
    print(f"Stats: {len(merged_name)} empresas, {len(merged_cnpj)} com CNPJ.")


if __name__ == "__main__":
    main()
