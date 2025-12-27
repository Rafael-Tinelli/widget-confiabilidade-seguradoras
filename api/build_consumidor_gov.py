from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, Iterable, Tuple

from api.sources.consumidor_gov import (
    Agg,
    aggregate_month,
    discover_basecompleta_urls,
    download_csv_to_gz,
)

CACHE_DIR = "data/.cache/consumidor_gov"  # não versionar
MONTHLY_DIR = "data/derived/consumidor_gov/monthly"
OUT_LATEST = "data/derived/consumidor_gov/consumidor_gov_agg_latest.json"


def _utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _ensure_dirs() -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(MONTHLY_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(OUT_LATEST), exist_ok=True)


def _monthly_path(ym: str) -> str:
    return os.path.join(MONTHLY_DIR, f"consumidor_gov_{ym}.json")


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _agg_from_raw(raw: Dict[str, Any]) -> Agg:
    a = Agg()
    a.display_name = raw.get("display_name", "") or ""
    a.total = int(raw.get("total", 0) or 0)
    a.finalizadas = int(raw.get("finalizadas", 0) or 0)
    a.respondidas = int(raw.get("respondidas", 0) or 0)
    a.resolvidas_indicador = int(raw.get("resolvidas_indicador", 0) or 0)
    a.nota_sum = float(raw.get("nota_sum", 0.0) or 0.0)
    a.nota_count = int(raw.get("nota_count", 0) or 0)
    a.tempo_sum = int(raw.get("tempo_sum", 0) or 0)
    a.tempo_count = int(raw.get("tempo_count", 0) or 0)
    return a


def _sum_aggs(items: Iterable[Tuple[str, Dict[str, Any]]]) -> Dict[str, Agg]:
    out: Dict[str, Agg] = {}
    for key, raw in items:
        if key not in out:
            out[key] = _agg_from_raw(raw)
            continue

        base = out[key]
        cur = _agg_from_raw(raw)

        base.total += cur.total
        base.finalizadas += cur.finalizadas
        base.respondidas += cur.respondidas
        base.resolvidas_indicador += cur.resolvidas_indicador
        base.nota_sum += cur.nota_sum
        base.nota_count += cur.nota_count
        base.tempo_sum += cur.tempo_sum
        base.tempo_count += cur.tempo_count

    return out


def _prune_monthly(retain: int = 36) -> None:
    files = sorted(
        [f for f in os.listdir(MONTHLY_DIR) if f.startswith("consumidor_gov_") and f.endswith(".json")]
    )
    if len(files) <= retain:
        return

    to_delete = files[: len(files) - retain]
    for f in to_delete:
        try:
            os.remove(os.path.join(MONTHLY_DIR, f))
        except OSError:
            pass


def main(months: int = 12) -> None:
    _ensure_dirs()

    urls = discover_basecompleta_urls(months=months)
    if not urls:
        raise SystemExit("Nenhuma URL de Base Completa encontrada (Consumidor.gov).")

    yms = sorted(urls.keys())  # crescente
    as_of = max(urls.keys())

    # 1) Garante agregados mensais (incremental)
    produced = []
    for ym in yms:
        out_month = _monthly_path(ym)
        if os.path.exists(out_month):
            continue

        url = urls[ym]
        gz_path = os.path.join(CACHE_DIR, f"basecompleta_{ym}.csv.gz")

        info = download_csv_to_gz(url, gz_path)
        aggs = aggregate_month(gz_path)

        payload = {
            "meta": {
                "ym": ym,
                "source": "dados.mj.gov.br",
                "dataset": "reclamacoes-do-consumidor-gov-br",
                "source_url": url,
                "download": info,
                "generated_at": _utc_now(),
            },
            # raw é o que permite somar meses sem perda
            "by_name_key_raw": {k: asdict(v) for k, v in aggs.items()},
        }

        _write_json(out_month, payload)
        produced.append(ym)

        # opcional: remover cache do mês após processar (poupa disco)
        try:
            os.remove(gz_path)
        except OSError:
            pass

    _prune_monthly(retain=max(36, months + 6))

    # 2) Monta janela (12m) somando os raw dos meses escolhidos
    month_payloads = []
    for ym in yms:
        p = _monthly_path(ym)
        if os.path.exists(p):
            month_payloads.append((ym, _load_json(p)))

    if not month_payloads:
        raise SystemExit("Nenhum agregado mensal disponível para montar a janela.")

    merged_items = []
    for ym, mp in month_payloads:
        raw = mp.get("by_name_key_raw", {})
        for k, v in raw.items():
            merged_items.append((k, v))

    merged = _sum_aggs(merged_items)

    out = {
        "meta": {
            "source": "dados.mj.gov.br",
            "dataset": "reclamacoes-do-consumidor-gov-br",
            "window_months": months,
            "months": yms,
            "as_of": as_of,
            "generated_at": _utc_now(),
            "monthly_newly_built": produced,
        },
        "by_name_key_raw": {k: asdict(v) for k, v in merged.items()},
        "by_name_key": {k: v.to_public() for k, v in merged.items()},
    }

    _write_json(OUT_LATEST, out)
    print(f"OK: {OUT_LATEST} (empresas={len(out['by_name_key'])}, as_of={as_of})")


if __name__ == "__main__":
    main()
