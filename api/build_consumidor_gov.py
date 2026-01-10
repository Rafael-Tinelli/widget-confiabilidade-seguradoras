from __future__ import annotations

import csv
import json
import os
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

try:
    # Preferir normalização do projeto para manter consistência com o matcher
    from api.utils.name_cleaner import normalize_name_key
except Exception:  # pragma: no cover
    def normalize_name_key(s: str) -> str:  # type: ignore
        s2 = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
        s2 = re.sub(r"[^a-zA-Z0-9]+", " ", s2).strip().lower()
        return re.sub(r"\s+", " ", s2)


DATASET_ID = os.getenv("CG_DATASET_ID", "reclamacoes-do-consumidor-gov-br")
API_BASE = os.getenv("CG_CKAN_API_BASE", "https://dados.mj.gov.br/api/3/action")

RAW_DIR = Path(os.getenv("CG_RAW_DIR", "data/raw/consumidor_gov"))
DERIVED_DIR = Path(os.getenv("CG_DERIVED_DIR", "data/derived/consumidor_gov"))
MONTHLY_DIR = Path(os.getenv("CG_DERIVED_MONTHLY_DIR", str(DERIVED_DIR / "monthly")))

TARGET_SEGMENT = os.getenv("CG_TARGET_SEGMENT", "Seguros, Capitalização e Previdência")
LAG_MONTHS = int(os.getenv("CG_LAG_MONTHS", "1"))
WINDOW_MONTHS = int(os.getenv("CG_WINDOW_MONTHS", "12"))
FORCE_REBUILD = os.getenv("CG_FORCE", "0") == "1"

RAW_DIR.mkdir(parents=True, exist_ok=True)
DERIVED_DIR.mkdir(parents=True, exist_ok=True)
MONTHLY_DIR.mkdir(parents=True, exist_ok=True)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


_MONTH_PT = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}


def _parse_resource_month(resource_name: str) -> tuple[int, int] | None:
    """
    Ex.: "Base Completa Consumidor.gov.br - Novembro_2025" -> (2025, 11)
    """
    n = _strip_accents(resource_name).lower()
    m = re.search(
        r"(janeiro|fevereiro|marco|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)[\s_-]*([12]\d{3})",
        n,
    )
    if not m:
        return None
    month = _MONTH_PT[m.group(1)]
    year = int(m.group(2))
    return year, month


def _add_months(year: int, month: int, delta: int) -> tuple[int, int]:
    idx = (year * 12 + (month - 1)) + delta
    y = idx // 12
    m = (idx % 12) + 1
    return y, m


def _window_months(now_utc: datetime, lag_months: int, window_months: int) -> list[str]:
    # Âncora: mês atual - lag. Em 2026-01 com lag=1 => 2025-12.
    anchor_y, anchor_m = _add_months(now_utc.year, now_utc.month, -lag_months)
    out: list[str] = []
    y, m = anchor_y, anchor_m
    for _ in range(window_months):
        out.append(f"{y:04d}-{m:02d}")
        y, m = _add_months(y, m, -1)
    return out


def _ckan_get_package(dataset_id: str) -> dict[str, Any]:
    url = f"{API_BASE}/package_show"
    resp = requests.get(url, params={"id": dataset_id}, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("success"):
        raise RuntimeError(f"CKAN package_show returned success=false: {payload}")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("CKAN package_show: missing result dict")
    return result


def _resource_map_by_month(resources: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """
    Mapa YYYY-MM -> resource. Se houver duplicata, pega o mais recente.
    """
    best: dict[str, dict[str, Any]] = {}
    for r in resources:
        name = str(r.get("name") or "")
        parsed = _parse_resource_month(name)
        if not parsed:
            continue
        y, m = parsed
        key = f"{y:04d}-{m:02d}"
        prev = best.get(key)
        if not prev:
            best[key] = r
            continue
        prev_ts = str(prev.get("last_modified") or prev.get("created") or "")
        cur_ts = str(r.get("last_modified") or r.get("created") or "")
        if cur_ts > prev_ts:
            best[key] = r
    return best


def _detect_delimiter(path: Path) -> str:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        sample = f.read(16384)
    return ";" if sample.count(";") >= sample.count(",") else ","


def _safe_float(v: Any) -> float:
    try:
        if v in (None, "", "NA", "N/A", "-"):
            return 0.0
        s = str(v).strip().replace(",", ".")
        return float(s)
    except Exception:
        return 0.0


def _aggregate_basecompleta_month(csv_path: Path, ym: str) -> dict[str, Any]:
    """
    CSV transacional (1 linha por reclamação) -> agregado por empresa (Nome Fantasia),
    filtrando por Segmento de Seguros.
    """
    delimiter = _detect_delimiter(csv_path)

    col_segment = "Segmento de Mercado"
    col_name = "Nome Fantasia"
    col_responded = "Respondida"
    col_status = "Situação"
    col_eval = "Avaliação Reclamação"
    col_score = "Nota do Consumidor"

    by_name_key: dict[str, dict[str, Any]] = {}

    processed = 0
    ignored_segment = 0
    ignored_cancelled = 0
    ignored_no_name = 0

    with csv_path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            seg = (row.get(col_segment) or "").strip()
            if seg != TARGET_SEGMENT:
                ignored_segment += 1
                continue

            status = (row.get(col_status) or "").strip()
            if "Cancelad" in status:  # Cancelada/Cancelado
                ignored_cancelled += 1
                continue

            name = (row.get(col_name) or row.get("Empresa") or row.get("Fornecedor") or "").strip()
            if not name:
                ignored_no_name += 1
                continue

            key = normalize_name_key(name)
            entry = by_name_key.get(key)
            if not entry:
                entry = {
                    "name": name,
                    "display_name": name,
                    "cnpj": None,  # Base completa não traz CNPJ da empresa reclamada
                    "statistics": {
                        "complaintsCount": 0,
                        "respondedCount": 0,
                        "resolvedCount": 0,
                        "finalizedCount": 0,
                        "scoreSum": 0.0,
                        "satisfactionCount": 0,
                    },
                }
                by_name_key[key] = entry

            st = entry["statistics"]
            st["complaintsCount"] += 1

            if (row.get(col_responded) or "").strip().upper() == "S":
                st["respondedCount"] += 1

            if "Finalizada" in status or "Encerrada" in status:
                st["finalizedCount"] += 1

            if (row.get(col_eval) or "").strip() == "Resolvida":
                st["resolvedCount"] += 1

            score = _safe_float(row.get(col_score))
            if score > 0:
                st["scoreSum"] += score
                st["satisfactionCount"] += 1

            processed += 1

    meta = {
        "status": "ok",
        "source": "consumidor.gov.br (Base Completa transacional)",
        "month": ym,
        "filter": {"Segmento de Mercado": TARGET_SEGMENT},
        "rows_processed": processed,
        "rows_ignored_segment": ignored_segment,
        "rows_ignored_cancelled": ignored_cancelled,
        "rows_ignored_no_name": ignored_no_name,
        "companies": len(by_name_key),
        "generated_at": _utc_now(),
        "source_file": str(csv_path),
    }

    # Compatibilidade com chaves legadas e com o matcher atual
    return {
        "meta": meta,
        "by_name_key_raw": by_name_key,
        "by_name_key": by_name_key,
        "by_name": by_name_key,
        "by_cnpj_key_raw": {},
        "by_cnpj_key": {},
        "by_cnpj": {},
    }


def _download_file(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        tmp = out_path.with_suffix(out_path.suffix + ".part")
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
        tmp.replace(out_path)


def _merge_monthlies(monthly_paths: list[Path], window: list[str]) -> dict[str, Any]:
    merged: dict[str, dict[str, Any]] = {}
    total_rows = 0

    for p in monthly_paths:
        root = json.loads(p.read_text(encoding="utf-8"))
        total_rows += int(root.get("meta", {}).get("rows_processed", 0))

        by_name = root.get("by_name_key_raw") or root.get("by_name") or {}
        if not isinstance(by_name, dict):
            continue

        for k, entry in by_name.items():
            if not isinstance(entry, dict):
                continue
            stats = entry.get("statistics") or {}
            if not isinstance(stats, dict):
                continue

            cur = merged.get(k)
            if not cur:
                merged[k] = {
                    "name": entry.get("name") or entry.get("display_name") or "",
                    "display_name": entry.get("display_name") or entry.get("name") or "",
                    "cnpj": entry.get("cnpj"),
                    "statistics": {
                        "complaintsCount": 0,
                        "respondedCount": 0,
                        "resolvedCount": 0,
                        "finalizedCount": 0,
                        "scoreSum": 0.0,
                        "satisfactionCount": 0,
                    },
                }
                cur = merged[k]

            st = cur["statistics"]
            st["complaintsCount"] += int(stats.get("complaintsCount", 0) or 0)
            st["respondedCount"] += int(stats.get("respondedCount", 0) or 0)
            st["resolvedCount"] += int(stats.get("resolvedCount", 0) or 0)
            st["finalizedCount"] += int(stats.get("finalizedCount", 0) or 0)
            st["scoreSum"] += float(stats.get("scoreSum", 0.0) or 0.0)
            st["satisfactionCount"] += int(stats.get("satisfactionCount", 0) or 0)

    meta = {
        "status": "ok",
        "dataset_id": DATASET_ID,
        "window": window,
        "companies": len(merged),
        "rows_processed_total": total_rows,
        "generated_at": _utc_now(),
    }

    return {
        "meta": meta,
        "by_name_key_raw": merged,
        "by_name_key": merged,
        "by_name": merged,
        "by_cnpj_key_raw": {},
        "by_cnpj_key": {},
        "by_cnpj": {},
    }


def main() -> int:
    now = datetime.now(timezone.utc)
    window = _window_months(now, LAG_MONTHS, WINDOW_MONTHS)

    print("--- BUILD CONSUMIDOR.GOV (BASECOMPLETA -> MONTHLY AGG) ---")
    print(f"CG: Janela alvo (Lag={LAG_MONTHS}): {window}")

    pkg = _ckan_get_package(DATASET_ID)
    resources = pkg.get("resources") or []
    if not isinstance(resources, list):
        raise RuntimeError("CKAN package_show: resources is not a list")

    by_month = _resource_map_by_month(resources)

    monthly_paths: list[Path] = []

    for ym in window:
        out_json = MONTHLY_DIR / f"consumidor_gov_{ym}.json"
        monthly_paths.append(out_json)

        if out_json.exists() and not FORCE_REBUILD:
            continue

        res = by_month.get(ym)
        if not res:
            print(f"CG WARN: recurso não encontrado para {ym} (provável mês não publicado)")
            continue

        url = str(res.get("url") or "")
        if not url:
            print(f"CG WARN: recurso {ym} sem url (resource id={res.get('id')})")
            continue

        raw_csv = RAW_DIR / f"basecompleta_{ym}.csv"
        if not raw_csv.exists() or FORCE_REBUILD:
            print(f"CG: baixando {ym} -> {raw_csv.name}")
            _download_file(url, raw_csv)

        print(f"CG: agregando {ym} (segmento={TARGET_SEGMENT})")
        monthly = _aggregate_basecompleta_month(raw_csv, ym)
        out_json.write_text(json.dumps(monthly, ensure_ascii=False), encoding="utf-8")
        print(f"CG: OK {ym} -> {out_json}")

    existing = [p for p in monthly_paths if p.exists()]
    if not existing:
        print(f"CG FAIL: Nenhum arquivo encontrado em {MONTHLY_DIR}. Janela: {window}")
        return 1

    agg = _merge_monthlies(existing, window)

    # Saídas agregadas (compatibilidade)
    (DERIVED_DIR / "consumidor_gov_agg.json").write_text(json.dumps(agg, ensure_ascii=False), encoding="utf-8")
    (DERIVED_DIR / "consumidor_gov_aggregated.json").write_text(json.dumps(agg, ensure_ascii=False), encoding="utf-8")

    print(f"CG: agregado -> {DERIVED_DIR / 'consumidor_gov_agg.json'} (companies={agg.get('meta', {}).get('companies')})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
