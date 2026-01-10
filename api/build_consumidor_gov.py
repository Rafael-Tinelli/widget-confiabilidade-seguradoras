# api/build_consumidor_gov.py
from __future__ import annotations

import json
import os
import re
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

# Importa do novo motor
from api.sources.consumidor_gov import Agg, sync_monthly_cache_from_dump_if_needed, _utc_now

DERIVED_DIR = "data/derived/consumidor_gov"
MONTHLY_DIR = f"{DERIVED_DIR}/monthly"
OUT_LATEST = f"{DERIVED_DIR}/aggregated.json"


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        raise SystemExit(f"CG FAIL: env {name} inválida (esperado int): {v!r}")


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        raise SystemExit(f"CG FAIL: env {name} inválida (esperado float): {v!r}")


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


def cg_assert(cond: bool, msg: str) -> None:
    if not cond:
        raise SystemExit(f"CG FAIL: {msg}")


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

    # -------------------------
    # Guardrails Config
    # -------------------------
    CG_MIN_MONTHS_FOUND = _env_int("CG_MIN_MONTHS_FOUND", max(3, months // 2))
    CG_MIN_TOTAL_COMPANIES = _env_int("CG_MIN_TOTAL_COMPANIES", 300)
    CG_MIN_CNPJ_ABS = _env_int("CG_MIN_CNPJ_ABS", 50)
    CG_MIN_CNPJ_PCT = _env_float("CG_MIN_CNPJ_PCT", 0.05)
    CG_ALLOW_NO_CNPJ = _env_bool("CG_ALLOW_NO_CNPJ", False)
    
    # Controle de Janela de Tempo
    CG_SKIP_CURRENT_MONTH = _env_bool("CG_SKIP_CURRENT_MONTH", True)
    CG_PUBLICATION_LAG = _env_int("CG_PUBLICATION_LAG", 3) # Pula +N meses para trás

    # 1. Definir Janela
    today = datetime.now(timezone.utc)
    
    def back_one_month(d: datetime) -> datetime:
        first_day = d.replace(day=1)
        return first_day - timedelta(days=1)

    # Lógica de Retrocesso:
    # 1. Se configurado, sai do mês corrente (ex: Jan -> Dez)
    if CG_SKIP_CURRENT_MONTH:
        today = back_one_month(today)
    
    # 2. Aplica o Lag de publicação (ex: Dez -> Nov)
    for _ in range(CG_PUBLICATION_LAG):
        today = back_one_month(today)

    target_yms = []
    for i in range(months):
        y = today.year
        m = today.month - i
        while m <= 0:
            m += 12
            y -= 1
        target_yms.append(f"{y:04d}-{m:02d}")

    cg_assert(len(target_yms) == months, f"Janela inconsistente: {len(target_yms)} != {months}")
    cg_assert(all(re.fullmatch(r"\d{4}-\d{2}", ym) for ym in target_yms), "Formato inválido em target_yms")

    print(f"CG: Janela alvo (Lag={CG_PUBLICATION_LAG}): {target_yms}")

    # 2. Sync
    os.makedirs(MONTHLY_DIR, exist_ok=True)
    sync_monthly_cache_from_dump_if_needed(target_yms, MONTHLY_DIR)

    # Assert pós-sync
    months_existing = [
        ym for ym in target_yms
        if os.path.exists(os.path.join(MONTHLY_DIR, f"consumidor_gov_{ym}.json"))
    ]
    cg_assert(
        len(months_existing) >= 1,
        f"Nenhum arquivo encontrado em {MONTHLY_DIR}. Janela: {target_yms}"
    )
    cg_assert(
        len(months_existing) >= CG_MIN_MONTHS_FOUND,
        f"Cobertura insuficiente: {len(months_existing)}/{months} meses. "
        f"Min: {CG_MIN_MONTHS_FOUND}. Encontrados: {months_existing}"
    )

    # 3. Merge
    merged_name: dict[str, Agg] = {}
    merged_cnpj: dict[str, Agg] = {}
    months_found = []
    has_cnpj_col_any = False

    for ym in target_yms:
        p = os.path.join(MONTHLY_DIR, f"consumidor_gov_{ym}.json")
        if not os.path.exists(p):
            continue

        try:
            month_data = _load_json(p)
            months_found.append(ym)

            cg_assert(isinstance(month_data, dict), f"{p}: JSON inválido")
            
            # Checa se a fonte tinha CNPJ
            parse_meta = (month_data.get("meta") or {}).get("parse") or {}
            has_cnpj_col_any = has_cnpj_col_any or bool(parse_meta.get("has_cnpj_col"))
            
            raw_n = month_data.get("by_name_key_raw")
            raw_c = month_data.get("by_cnpj_key_raw")

            cg_assert(isinstance(raw_n, dict) and len(raw_n) > 0, f"{p}: by_name_key_raw vazio/inválido")
            
            if raw_c is None:
                raw_c = {}

            for k, v in raw_n.items():
                _merge_raw_into(merged_name, k, v)
            for k, v in raw_c.items():
                _merge_raw_into(merged_cnpj, k, v)

        except Exception as e:
            print(f"CG: Erro lendo {p}: {e}")

    # 4. Guard Rails Finais
    cg_assert(bool(merged_name), "Nenhum dado consolidado.")
    cg_assert(
        len(merged_name) >= CG_MIN_TOTAL_COMPANIES,
        f"Poucas empresas: {len(merged_name)} < {CG_MIN_TOTAL_COMPANIES}"
    )

    # CNPJ Check (Condicional)
    if len(merged_cnpj) == 0 and not CG_ALLOW_NO_CNPJ:
        if has_cnpj_col_any:
            raise SystemExit(
                "CG FAIL: 0 empresas com CNPJ. A fonte tinha coluna de CNPJ, então isso indica falha no parse. "
                "Verifique URL/colunas. Use CG_ALLOW_NO_CNPJ=1 apenas em emergência."
            )
        else:
            print(
                "CG: WARN - Dumps mensais não possuem coluna de CNPJ. "
                "Prosseguindo apenas por chave de nome (name_key)."
            )
    
    if len(merged_cnpj) > 0:
        pct = len(merged_cnpj) / max(1, len(merged_name))
        cg_assert(
            len(merged_cnpj) >= CG_MIN_CNPJ_ABS or pct >= CG_MIN_CNPJ_PCT,
            f"Baixa cobertura CNPJ: {len(merged_cnpj)} ({pct:.1%}). "
            f"Req: abs>={CG_MIN_CNPJ_ABS} ou pct>={CG_MIN_CNPJ_PCT:.1%}"
        )

# 5. Exportação
    by_name_data = {k: v.to_public() for k, v in merged_name.items()}

    out = {
        "meta": {
            "generated_at": _utc_now(),
            "window_months": len(months_found),
            "months": months_found,
            "source": "consumidor.gov.br (Dump)",
            "stats": {
                "total_companies": len(merged_name),
                "companies_with_cnpj": len(merged_cnpj),
                "source_has_cnpj_column": bool(has_cnpj_col_any),
            }
        },
        "by_name": by_name_data,
        "by_name_key": by_name_data,  # [PATCH] Alias para compatibilidade
        "by_cnpj_key": {k: v.to_public() for k, v in merged_cnpj.items()},
        "by_name_key_raw": {k: asdict(v) for k, v in merged_name.items()}
    }

    os.makedirs(os.path.dirname(OUT_LATEST), exist_ok=True)
    with open(OUT_LATEST, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(',', ':'))

    cg_assert(os.path.exists(OUT_LATEST) and os.path.getsize(OUT_LATEST) > 5000, "Arquivo final inválido")

    print(f"OK: Agregado salvo em {OUT_LATEST}")
    print(f"Stats: {len(merged_name)} empresas, {len(merged_cnpj)} com CNPJ.")


if __name__ == "__main__":
    main()
