# api/build_consumidor_gov.py
from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

# Importa o novo motor híbrido
from api.sources.consumidor_gov import Agg, sync_monthly_cache_from_dump, _utc_now

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
    
    # 1. AUTO-DISCOVERY: Tenta baixar o dump e gerar os mensais
    # Isso preenche a pasta 'data/derived/consumidor_gov/monthly'
    print("CG: Verificando/Atualizando cache mensal via Dump...")
    sync_monthly_cache_from_dump(MONTHLY_DIR)

    # 2. DEFINIR JANELA DE TEMPO
    # Pega os últimos X arquivos JSON disponíveis na pasta monthly
    available_files = sorted(Path(MONTHLY_DIR).glob("consumidor_gov_*.json"))
    if not available_files:
        print("CG: ALERTA CRÍTICO - Nenhum arquivo mensal encontrado após sync.")
        # Não aborta ainda, tenta processar o que tem
    
    # Pega os últimos 'months' arquivos (ex: 12)
    selected_files = available_files[-months:]
    merge_yms = [f.stem.replace("consumidor_gov_", "") for f in selected_files]
    
    print(f"CG: Meses selecionados para fusão: {merge_yms}")

    merged_name: dict[str, Agg] = {}
    merged_cnpj: dict[str, Agg] = {}
    
    # 3. FUSÃO (MERGE)
    for p in selected_files:
        try:
            month = _load_json(str(p))
        except Exception as e:
            print(f"CG: Erro lendo {p}: {e}")
            continue

        raw_name = month.get("by_name_key_raw") or {}
        raw_cnpj = month.get("by_cnpj_key_raw") or {}

        for k, raw in raw_name.items():
            if isinstance(raw, dict):
                _merge_raw_into(merged_name, str(k), raw)

        for k, raw in raw_cnpj.items():
            if isinstance(raw, dict):
                _merge_raw_into(merged_cnpj, str(k), raw)

    # 4. GUARD RAIL (TRAVA DE SEGURANÇA)
    if len(merged_name) == 0:
        print("CG: ERRO FATAL - Nenhum dado processado após fusão.")
        if os.path.exists(OUT_LATEST):
            print("CG: Mantendo arquivo anterior para não quebrar o site.")
        sys.exit(1) # Falha o GitHub Action propositalmente

    # 5. EXPORTAÇÃO
    out = {
        "meta": {
            "generated_at": _utc_now(),
            "months_used": merge_yms,
            "source": "consumidor.gov.br (Dump)",
            "cnpj_matches": len(merged_cnpj)
        },
        # Converte objetos Agg para dicts públicos
        "by_name": {k: v.to_public() for k, v in merged_name.items()},
        "by_cnpj_key": {k: v.to_public() for k, v in merged_cnpj.items()},
        
        # Mantém raws para compatibilidade futura se precisar
        "by_name_key_raw": {k: asdict(v) for k, v in merged_name.items()},
    }

    os.makedirs(os.path.dirname(OUT_LATEST), exist_ok=True)
    with open(OUT_LATEST, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(',', ':'))

    print(f"OK: Consumidor.gov agregado salvo em {OUT_LATEST}")
    print(f"Stats: {len(merged_name)} empresas por nome, {len(merged_cnpj)} por CNPJ.")

if __name__ == "__main__":
    main()
