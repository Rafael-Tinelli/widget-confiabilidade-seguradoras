from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime
from typing import Any

from api.sources.consumidor_gov import (
    Agg,
    aggregate_month_dual,
    discover_basecompleta_urls,
    download_csv_to_gz,
)

CACHE_DIR = "data/.cache/consumidor_gov"
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


def _load_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, payload: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _read_current_as_of() -> str | None:
    if not os.path.exists(OUT_LATEST):
        return None
    try:
        meta = _load_json(OUT_LATEST).get("meta", {})
        val = meta.get("as_of")
        return str(val) if val else None
    except Exception:
        return None


def _latest_available_month() -> str | None:
    latest_urls = discover_basecompleta_urls(months=1)
    return next(iter(latest_urls.keys()), None)


def _agg_from_raw(raw: dict[str, Any]) -> Agg:
    return Agg(
        display_name=str(raw.get("display_name") or ""),
        total=int(raw.get("total", 0) or 0),
        finalizadas=int(raw.get("finalizadas", 0) or 0),
        respondidas=int(raw.get("respondidas", 0) or 0),
        resolvidas_indicador=int(raw.get("resolvidas_indicador", 0) or 0),
        nota_sum=float(raw.get("nota_sum", 0.0) or 0.0),
        nota_count=int(raw.get("nota_count", 0) or 0),
        tempo_sum=float(raw.get("tempo_sum", 0.0) or 0.0),
        tempo_count=int(raw.get("tempo_count", 0) or 0),
    )


def _merge_raw_into(target: dict[str, Agg], key: str, raw: dict[str, Any]) -> None:
    cur = _agg_from_raw(raw)
    if key not in target:
        target[key] = cur
        return
    target[key].merge(cur)


def _prune_monthly(retain: int) -> None:
    files = sorted(
        f
        for f in os.listdir(MONTHLY_DIR)
        if f.startswith("consumidor_gov_") and f.endswith(".json")
    )
    if len(files) <= retain:
        return
    for f in files[: len(files) - retain]:
        try:
            os.remove(os.path.join(MONTHLY_DIR, f))
        except OSError:
            pass


def main(months: int = 12) -> None:
    _ensure_dirs()

    if os.environ.get("CONSUMIDOR_GOV_FORCE") != "1":
        latest = _latest_available_month()
        current = _read_current_as_of()
        if latest and current and latest == current and os.path.exists(_monthly_path(latest)):
            print(f"OK: Consumidor.gov já atualizado (as_of={current}). Pulando rebuild.")
            return

    urls = discover_basecompleta_urls(months=months)
    if not urls:
        raise SystemExit("Nenhuma URL de Base Completa encontrada (Consumidor.gov).")

    yms = sorted(urls.keys())
    as_of = max(urls.keys())

    produced: list[str] = []
    for ym in yms:
        out_month = _monthly_path(ym)
        if os.path.exists(out_month):
            continue

        url = urls[ym]
        gz_path = os.path.join(CACHE_DIR, f"basecompleta_{ym}.csv.gz")

        info = download_csv_to_gz(url, gz_path)
        by_name, by_cnpj = aggregate_month_dual(gz_path)

        payload_month: dict[str, Any] = {
            "meta": {
                "ym": ym,
                "source": "dados.mj.gov.br",
                "dataset": "reclamacoes-do-consumidor-gov-br",
                "source_url": url,
                "download": info,
                "generated_at": _utc_now(),
            },
            "by_name_key_raw": {k: asdict(v) for k, v in by_name.items()},
            "by_cnpj_key_raw": {k: asdict(v) for k, v in by_cnpj.items()},
        }

        _write_json(out_month, payload_month)
        produced.append(ym)

        try:
            os.remove(gz_path)
        except OSError:
            pass

    _prune_monthly(retain=max(36, months + 6))

    merged_name: dict[str, Agg] = {}
    merged_cnpj: dict[str, Agg] = {}

    for ym in yms:
        p = _monthly_path(ym)
        if not os.path.exists(p):
            continue
        mp = _load_json(p)

        raw_name = mp.get("by_name_key_raw", {})
        if isinstance(raw_name, dict):
            for k, raw in raw_name.items():
                if isinstance(raw, dict):
                    _merge_raw_into(merged_name, str(k), raw)

        raw_cnpj = mp.get("by_cnpj_key_raw", {})
        if isinstance(raw_cnpj, dict):
            for k, raw in raw_cnpj.items():
                if isinstance(raw, dict):
                    _merge_raw_into(merged_cnpj, str(k), raw)

    if not merged_name:
        raise SystemExit("Nenhum agregado mensal disponível para montar a janela.")

    out: dict[str, Any] = {
        "meta": {
            "source": "dados.mj.gov.br",
            "dataset": "reclamacoes-do-consumidor-gov-br",
            "window_months": months,
            "months": yms,
