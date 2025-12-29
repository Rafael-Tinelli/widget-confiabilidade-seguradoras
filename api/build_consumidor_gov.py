import json
import os
from dataclasses import asdict
from pathlib import Path

from api.sources.consumidor_gov import (
    Agg,
    aggregate_month_dual_with_stats,
    download_month_csv_gz,
    _utc_now,
)

RAW_DIR = "data/raw/consumidor_gov"
DERIVED_DIR = "data/derived/consumidor_gov"
MONTHLY_DIR = f"{DERIVED_DIR}/monthly"

OUT_LATEST = f"{DERIVED_DIR}/consumidor_gov_agg_latest.json"


def _month_key(dt: str) -> str:
    # dt pode vir yyyy-mm-dd; a gente usa yyyy-mm
    return str(dt or "")[:7]


def _monthly_path(ym: str) -> str:
    return os.path.join(MONTHLY_DIR, f"consumidor_gov_{ym}.json")


def _write_json(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=False)
        f.write("\n")
    os.replace(tmp, path)


def _load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _prune_monthly(retain: int = 24) -> None:
    if not os.path.isdir(MONTHLY_DIR):
        return
    files = sorted(Path(MONTHLY_DIR).glob("consumidor_gov_*.json"))
    if len(files) <= retain:
        return
    for p in files[:-retain]:
        try:
            p.unlink()
        except Exception:
            pass


def _merge_raw_into(dst: dict[str, Agg], key: str, raw: dict) -> None:
    a = dst.get(key)
    if not a:
        a = Agg(display_name=str(raw.get("display_name") or ""))
        dst[key] = a
    a.merge_raw(raw)


def build_month(ym: str, url: str) -> str:
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(MONTHLY_DIR, exist_ok=True)

    gz_path = os.path.join(RAW_DIR, f"consumidor_gov_{ym}.csv.gz")
    info = download_month_csv_gz(url, gz_path)

    # Usa a nova função que retorna estatísticas de parsing
    by_name, by_cnpj, stats = aggregate_month_dual_with_stats(gz_path)

    out_path = _monthly_path(ym)
    payload = {
        "meta": {
            "ym": ym,
            "source": "dados.mj.gov.br",
            "dataset": "reclamacoes-do-consumidor-gov-br",
            "source_url": url,
            "download": info,
            "generated_at": _utc_now(),
            # Salva estatísticas cruas
            "parse": stats,
            # Bloco explícito de qualidade do CNPJ para este mês
            "cnpj": {
                "detected_column": stats.get("cnpj_col"),
                "rows_with_cnpj_valid": stats.get("rows_with_cnpj_valid"),
                "unique_cnpj_keys": stats.get("unique_cnpj_keys"),
            },
        },
        "by_name_key_raw": {k: asdict(v) for k, v in by_name.items()},
        "by_cnpj_key_raw": {k: asdict(v) for k, v in by_cnpj.items()},
    }

    _write_json(out_path, payload)
    return out_path


def main(months: int = 12) -> None:
    """
    Gera:
      - monthly/<yyyy-mm>.json (raw aggregates por nome e por CNPJ)
      - consumidor_gov_agg_latest.json (rolling window consolidado)
    """
    # A URL base pode ser injetada por env (mantém flexibilidade)
    base_url = os.environ.get("CONSUMIDOR_GOV_BASE_URL", "").strip()
    if not base_url:
        # padrão (ajuste conforme seu discovery atual)
        base_url = "https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br/resource"

    # merge_yms pode vir pronto via env; senão assume últimos N meses a partir de as_of
    env_as_of = os.environ.get("CONSUMIDOR_GOV_AS_OF", "").strip()
    if env_as_of:
        as_of = env_as_of[:7]
    else:
        # fallback: tenta inferir pelo último arquivo monthly já existente; senão usa mês atual (UTC)
        existing = sorted(Path(MONTHLY_DIR).glob("consumidor_gov_*.json"))
        if existing:
            as_of = existing[-1].stem.replace("consumidor_gov_", "")[:7]
        else:
            as_of = _utc_now()[:7]

    # monta lista de meses
    if os.environ.get("CONSUMIDOR_GOV_MONTHS_LIST", "").strip():
        merge_yms = [x.strip() for x in os.environ["CONSUMIDOR_GOV_MONTHS_LIST"].split(",") if x.strip()]
    else:
        y, m = map(int, as_of.split("-"))
        # últimos N meses incluindo o as_of
        merge_yms = []
        yy, mm = y, m
        for _ in range(months):
            merge_yms.append(f"{yy:04d}-{mm:02d}")
            mm -= 1
            if mm == 0:
                mm = 12
                yy -= 1
        merge_yms = list(reversed(merge_yms))

    # Build monthly (se não existir)
    produced: list[str] = []
    for ym in merge_yms:
        out_path = _monthly_path(ym)
        if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
            continue

        # resource_id pode ser injetado por env de forma "ym=uuid,ym=uuid"
        mapping = os.environ.get("CONSUMIDOR_GOV_RESOURCE_MAP", "").strip()
        resource_id = ""
        if mapping:
            for part in mapping.split(","):
                part = part.strip()
                if not part or "=" not in part:
                    continue
                k, v = part.split("=", 1)
                if k.strip() == ym:
                    resource_id = v.strip()
                    break

        # fallback: assume que url já aponta para o CSV daquele mês (quando base_url for resolvido externamente)
        if resource_id:
            url = f"{base_url}/{resource_id}/download?filename=consumidor_gov_{ym}.csv.gz"
        else:
            url = os.environ.get(f"CONSUMIDOR_GOV_URL_{ym.replace('-', '_')}", "").strip()
            if not url:
                # sem URL definida, pula (não quebra o pipeline; smoke-check cobre tamanho mínimo)
                continue

        p = build_month(ym, url)
        produced.append(ym)
        print(f"OK: monthly {ym} => {p}")

    # Merge rolling window
    merged_name: dict[str, Agg] = {}
    merged_cnpj: dict[str, Agg] = {}
    
    used_months: list[str] = []

    # Estatísticas de merge para o agregado
    cnpj_detected_months: list[str] = []
    cnpj_col_counts: dict[str, int] = {}
    cnpj_rows_valid_total: int = 0

    for ym in merge_yms:
        p = _monthly_path(ym)
        if not os.path.exists(p):
            continue
        try:
            month = _load_json(p)
        except Exception:
            continue

        # Coleta estatísticas de parsing do mês para o agregado
        meta = month.get("meta") or {}
        parse_stats = meta.get("parse") if isinstance(meta.get("parse"), dict) else {}
        
        # Se houve coluna CNPJ detectada neste mês, registra
        ccol = parse_stats.get("cnpj_col")
        if ccol:
            cnpj_detected_months.append(ym)
            cnpj_col_counts[str(ccol)] = cnpj_col_counts.get(str(ccol), 0) + 1
        
        try:
            cnpj_rows_valid_total += int(parse_stats.get("rows_with_cnpj_valid") or 0)
        except (ValueError, TypeError):
            pass

        raw_name = month.get("by_name_key_raw") or {}
        raw_cnpj = month.get("by_cnpj_key_raw") or {}
        
        if not isinstance(raw_name, dict) or not raw_name:
            continue

        for k, raw in raw_name.items():
            if isinstance(raw, dict):
                _merge_raw_into(merged_name, str(k), raw)

        if isinstance(raw_cnpj, dict):
            for k, raw in raw_cnpj.items():
                if isinstance(raw, dict):
                    _merge_raw_into(merged_cnpj, str(k), raw)

        used_months.append(ym)

    # Determina a coluna mais frequente detectada (apenas informativo)
    most_freq_col = None
    if cnpj_col_counts:
        most_freq_col = max(cnpj_col_counts, key=cnpj_col_counts.get)

    out = {
        "meta": {
            "as_of": as_of,
            "window_months": len(used_months) if used_months else len(merge_yms),
            "months": used_months if used_months else merge_yms,
            "generated_at": _utc_now(),
            "produced_months": produced,
            "source": "dados.mj.gov.br",
            "dataset": "reclamacoes-do-consumidor-gov-br",
            # Bloco de Diagnóstico do Agregado
            "cnpj": {
                "detected_months": sorted(set(cnpj_detected_months)),
                "detected_column_most_freq": most_freq_col,
                "rows_with_cnpj_valid_total": cnpj_rows_valid_total,
                "unique_keys": len(merged_cnpj),
            },
        },
        # Mantém dados Raw para debug/compatibilidade
        "by_name_key_raw": {k: asdict(v) for k, v in merged_name.items()},
        "by_cnpj_key_raw": {k: asdict(v) for k, v in merged_cnpj.items()},
        # Dados públicos formatados
        "by_name_key": {k: v.to_public() for k, v in merged_name.items()},
        "by_cnpj_key": {k: v.to_public() for k, v in merged_cnpj.items()},
    }

    _write_json(OUT_LATEST, out)

    # Mantém uma folga (24) pra evitar ficar recriando mês antigo e pra dar fallback local
    _prune_monthly(retain=max(24, months))
    print(f"OK: Consumidor.gov agregado atualizado (as_of={as_of}). Keys CNPJ={len(merged_cnpj)}")


if __name__ == "__main__":
    env_months = os.environ.get("CONSUMIDOR_GOV_MONTHS")
    if env_months:
        try:
            main(months=int(env_months))
        except ValueError:
            main()
    else:
        main()
