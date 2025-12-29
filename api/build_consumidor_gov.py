# api/build_consumidor_gov.py
from __future__ import annotations

import json
import os
from dataclasses import asdict
from pathlib import Path

from api.sources.consumidor_gov import Agg, aggregate_month_dual_with_stats, download_month_csv_gz, _utc_now

RAW_DIR = "data/raw/consumidor_gov"
DERIVED_DIR = "data/derived/consumidor_gov"
MONTHLY_DIR = f"{DERIVED_DIR}/monthly"

OUT_LATEST = f"{DERIVED_DIR}/consumidor_gov_agg_latest.json"


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
            "parse": stats,
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
    base_url = os.environ.get("CONSUMIDOR_GOV_BASE_URL", "").strip()
    if not base_url:
        base_url = "https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br/resource"

    env_as_of = os.environ.get("CONSUMIDOR_GOV_AS_OF", "").strip()
    if env_as_of:
        as_of = env_as_of[:7]
    else:
        existing = sorted(Path(MONTHLY_DIR).glob("consumidor_gov_*.json"))
        as_of = existing[-1].stem.replace("consumidor_gov_", "")[:7] if existing else _utc_now()[:7]

    months_list = os.environ.get("CONSUMIDOR_GOV_MONTHS_LIST", "").strip()
    if months_list:
        merge_yms = [x.strip() for x in months_list.split(",") if x.strip()]
    else:
        y, m = map(int, as_of.split("-"))
        merge_yms: list[str] = []
        yy, mm = y, m
        for _ in range(months):
            merge_yms.append(f"{yy:04d}-{mm:02d}")
            mm -= 1
            if mm == 0:
                mm = 12
                yy -= 1
        merge_yms = list(reversed(merge_yms))

    produced_months: list[str] = []

    for ym in merge_yms:
        out_path = _monthly_path(ym)
        if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
            continue

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

        if resource_id:
            url = f"{base_url}/{resource_id}/download?filename=consumidor_gov_{ym}.csv.gz"
        else:
            url = os.environ.get(f"CONSUMIDOR_GOV_URL_{ym.replace('-', '_')}", "").strip()
            if not url:
                continue

        p = build_month(ym, url)
        produced_months.append(ym)
        print(f"OK: monthly {ym} => {p}")

    merged_name: dict[str, Agg] = {}
    merged_cnpj: dict[str, Agg] = {}
    used_months: list[str] = []

    cnpj_detected_months: list[str] = []
    cnpj_col_counts: dict[str, int] = {}
    cnpj_rows_valid_total = 0

    for ym in merge_yms:
        p = _monthly_path(ym)
        if not os.path.exists(p):
            continue
        try:
            month = _load_json(p)
        except Exception:
            continue

        meta = month.get("meta") or {}
        parse_stats = meta.get("parse") if isinstance(meta.get("parse"), dict) else {}

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

    most_freq_col = max(cnpj_col_counts, key=cnpj_col_counts.get) if cnpj_col_counts else None

    out = {
        "meta": {
            "as_of": as_of,
            "window_months": len(used_months) if used_months else len(merge_yms),
            "months": used_months if used_months else merge_yms,
            "generated_at": _utc_now(),
            "monthly_newly_built": produced_months,  # compat
            "produced_months": produced_months,
            "source": "dados.mj.gov.br",
            "dataset": "reclamacoes-do-consumidor-gov-br",
            "cnpj": {
                "detected_months": sorted(set(cnpj_detected_months)),
                "detected_column_most_freq": most_freq_col,
                "rows_with_cnpj_valid_total": cnpj_rows_valid_total,
                "unique_keys": len(merged_cnpj),
            },
        },
        "by_name_key_raw": {k: asdict(v) for k, v in merged_name.items()},
        "by_cnpj_key_raw": {k: asdict(v) for k, v in merged_cnpj.items()},
        "by_name_key": {k: v.to_public() for k, v in merged_name.items()},
        "by_cnpj_key": {k: v.to_public() for k, v in merged_cnpj.items()},
    }

    _write_json(OUT_LATEST, out)
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
