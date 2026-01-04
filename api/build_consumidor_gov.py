# api/build_consumidor_gov.py
from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import asdict
from datetime import datetime

# Importa do novo motor (com nome correto)
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
    # Guardrails configuráveis
    # -------------------------
    # Ajuste via ENV se precisar afrouxar/endereçar transição.
    CG_MIN_MONTHS_FOUND = _env_int("CG_MIN_MONTHS_FOUND", max(3, months // 2))
    CG_MIN_TOTAL_COMPANIES = _env_int("CG_MIN_TOTAL_COMPANIES", 300)
    CG_MIN_CNPJ_ABS = _env_int("CG_MIN_CNPJ_ABS", 50)
    CG_MIN_CNPJ_PCT = _env_float("CG_MIN_CNPJ_PCT", 0.05)  # 5%
    CG_ALLOW_NO_CNPJ = _env_int("CG_ALLOW_NO_CNPJ", 0) == 1  # emergência

    cg_assert(isinstance(months, int) and 1 <= months <= 36, f"months inválido: {months!r} (esperado 1..36)")

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

    cg_assert(len(target_yms) == months, f"janela gerada inconsistente: {len(target_yms)} != {months}")
    cg_assert(all(re.fullmatch(r"\d{4}-\d{2}", ym) for ym in target_yms), f"formato inválido em target_yms: {target_yms}")

    print(f"CG: Janela alvo: {target_yms}")

    # 2. Sync: Baixa dump e processa (se faltar mês)
    os.makedirs(MONTHLY_DIR, exist_ok=True)
    sync_monthly_cache_from_dump_if_needed(target_yms, MONTHLY_DIR)

    # Assert pós-sync: precisa existir pelo menos parte relevante da janela
    months_existing = [
        ym for ym in target_yms
        if os.path.exists(os.path.join(MONTHLY_DIR, f"consumidor_gov_{ym}.json"))
    ]
    cg_assert(
        len(months_existing) >= 1,
        f"nenhum arquivo mensal encontrado em {MONTHLY_DIR} após sync. Janela: {target_yms}"
    )
    cg_assert(
        len(months_existing) >= CG_MIN_MONTHS_FOUND,
        f"cobertura insuficiente: {len(months_existing)}/{months} meses disponíveis. "
        f"MIN={CG_MIN_MONTHS_FOUND}. Encontrados: {months_existing}"
    )

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

            cg_assert(isinstance(month_data, dict), f"{p}: JSON não é dict")

            raw_n = month_data.get("by_name_key_raw")
            raw_c = month_data.get("by_cnpj_key_raw")

            # Assert de schema mínimo: by_name_key_raw não pode faltar nem ser vazio
            cg_assert(isinstance(raw_n, dict), f"{p}: by_name_key_raw ausente ou inválido (type={type(raw_n).__name__})")
            cg_assert(len(raw_n) > 0, f"{p}: by_name_key_raw vazio")

            # by_cnpj_key_raw pode vir vazio em algum mês, mas não pode ser inválido
            cg_assert(raw_c is None or isinstance(raw_c, dict), f"{p}: by_cnpj_key_raw inválido (type={type(raw_c).__name__})")
            if raw_c is None:
                raw_c = {}

            for k, v in raw_n.items():
                cg_assert(isinstance(k, str) and k.strip(), f"{p}: chave vazia em by_name_key_raw")
                cg_assert(isinstance(v, dict), f"{p}: valor inválido em by_name_key_raw[{k!r}] (type={type(v).__name__})")
                _merge_raw_into(merged_name, k, v)
            for k, v in raw_c.items():
                cg_assert(isinstance(k, str) and k.strip(), f"{p}: chave vazia em by_cnpj_key_raw")
                cg_assert(isinstance(v, dict), f"{p}: valor inválido em by_cnpj_key_raw[{k!r}] (type={type(v).__name__})")
                _merge_raw_into(merged_cnpj, k, v)

        except Exception as e:
            print(f"CG: Erro lendo {p}: {e}")

    # 4. Guard Rail: Validação de Segurança do Agregado Final
    cg_assert(bool(merged_name), "nenhum dado consolidado em merged_name. Abortando.")
    cg_assert(
        len(merged_name) >= CG_MIN_TOTAL_COMPANIES,
        f"poucas empresas no agregado: {len(merged_name)} < MIN {CG_MIN_TOTAL_COMPANIES}. "
        f"Meses: {months_found}"
    )

    # Assert crítico: CNPJ é o indicador de que o pipeline de dump/parse está correto.
    if len(merged_cnpj) == 0 and not CG_ALLOW_NO_CNPJ:
        raise SystemExit(
            "CG FAIL: 0 empresas com CNPJ no agregado. "
            "Isso indica falha no download/parse (ex.: coluna CNPJ não capturada, dump errado/HTML, "
            "ou erro no sync). Defina CG_ALLOW_NO_CNPJ=1 apenas para emergência."
        )
    
    if len(merged_cnpj) > 0:
        pct = len(merged_cnpj) / max(1, len(merged_name))
        cg_assert(
            len(merged_cnpj) >= CG_MIN_CNPJ_ABS or pct >= CG_MIN_CNPJ_PCT,
            f"cobertura de CNPJ baixa: {len(merged_cnpj)}/{len(merged_name)} ({pct:.1%}). "
            f"Requer abs>={CG_MIN_CNPJ_ABS} ou pct>={CG_MIN_CNPJ_PCT:.1%}"
        )

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

    cg_assert(os.path.exists(OUT_LATEST), f"falha ao gravar {OUT_LATEST}")
    cg_assert(os.path.getsize(OUT_LATEST) > 5_000, f"arquivo final pequeno demais: {OUT_LATEST} ({os.path.getsize(OUT_LATEST)} bytes)")

    print(f"OK: Agregado salvo em {OUT_LATEST}")
    print(f"Stats: {len(merged_name)} empresas, {len(merged_cnpj)} com CNPJ.")


if __name__ == "__main__":
    main()
