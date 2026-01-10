# api/build_consumidor_gov.py
from __future__ import annotations

import csv
import gzip
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

try:
    # Usa a normalização do projeto (preferível para consistência com o matcher)
    from api.utils.name_cleaner import normalize_name_key
except Exception:  # pragma: no cover
    # Fallback ultra-simples (não ideal, mas evita hard-fail se o import quebrar)
    def normalize_name_key(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


CG_DATASET_ID = os.getenv("CG_DATASET_ID", "reclamacoes-do-consumidor-gov-br")
CG_API_BASE = os.getenv("CG_CKAN_API_BASE", "https://dados.mj.gov.br/api/3/action")

RAW_DIR = Path(os.getenv("CG_RAW_DIR", "data/raw/consumidor_gov"))
DERIVED_DIR = Path(os.getenv("CG_DERIVED_DIR", "data/derived/consumidor_gov"))
MONTHLY_DIR = Path(os.getenv("CG_DERIVED_MONTHLY_DIR", str(DERIVED_DIR / "monthly")))

MONTHS_BACK = int(os.getenv("CG_MONTHS_BACK", "12"))
FORCE_MONTH = os.getenv("CG_FORCE_MONTH")  # ex: "2025-12"
FORCE_DOWNLOAD = os.getenv("CG_FORCE_DOWNLOAD", "0").strip() == "1"

TARGET_SEGMENT = os.getenv("CG_TARGET_SEGMENT", "Seguros, Capitalização e Previdência").strip()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _month_to_date(ym: str) -> date:
    y, m = ym.split("-")
    return date(int(y), int(m), 1)


def _add_months(d: date, delta: int) -> date:
    # delta pode ser negativo
    y = d.year + (d.month - 1 + delta) // 12
    m = (d.month - 1 + delta) % 12 + 1
    return date(y, m, 1)


def _ym(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _safe_float(v: Any) -> float:
    try:
        if v in (None, "", "NA", "N/A", "-", "nan"):
            return 0.0
        return float(str(v).replace(",", "."))
    except Exception:
        return 0.0


def _ckan_get(action: str, params: dict[str, Any]) -> dict[str, Any]:
    url = f"{CG_API_BASE.rstrip('/')}/{action}"
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        raise RuntimeError(f"CKAN action failed: {action} {data}")
    return data["result"]


@dataclass(frozen=True)
class ResourceInfo:
    month: str
    name: str
    url: str
    format: str | None = None


_MONTH_RE = re.compile(r"(20\d{2})[-_/]?(\d{2})")


def _extract_month(text: str) -> str | None:
    """
    Extrai YYYY-MM de nomes do tipo:
      - basecompleta2025-12
      - ... 2025_12 ...
    """
    t = _norm(text)
    m = _MONTH_RE.search(t)
    if not m:
        return None
    year = int(m.group(1))
    month = int(m.group(2))
    if month < 1 or month > 12:
        return None
    return f"{year:04d}-{month:02d}"


def _list_basecompleta_resources() -> dict[str, ResourceInfo]:
    """
    Lê o dataset no CKAN e monta um mapa month->resource para Base Completa.
    """
    pkg = _ckan_get("package_show", {"id": CG_DATASET_ID})
    resources = pkg.get("resources") or []
    out: dict[str, ResourceInfo] = {}

    for res in resources:
        if not isinstance(res, dict):
            continue
        name = str(res.get("name") or res.get("title") or "")
        url = str(res.get("url") or "")
        fmt = str(res.get("format") or "").strip() or None

        hay = f"{name} {url}".lower()
        if "basecompleta" not in hay and "base completa" not in hay:
            continue

        month = _extract_month(hay)
        if not month:
            continue

        # preferir CSV quando houver duplicidade
        prev = out.get(month)
        if prev:
            new_is_csv = (fmt or "").lower() == "csv" or url.lower().endswith(".csv")
            prev_is_csv = (prev.format or "").lower() == "csv" or prev.url.lower().endswith(".csv")
            if new_is_csv and not prev_is_csv:
                out[month] = ResourceInfo(month=month, name=name, url=url, format=fmt)
        else:
            out[month] = ResourceInfo(month=month, name=name, url=url, format=fmt)

    return out


def _download(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        tmp = out_path.with_suffix(out_path.suffix + ".tmp")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024 * 4):  # 4MB
                if chunk:
                    f.write(chunk)
        tmp.replace(out_path)


def _iter_rows(csv_path: Path) -> Iterable[dict[str, str]]:
    """
    Itera linhas do CSV com autodetecção de encoding e delimitador.
    """
    encodings = ["utf-8", "utf-8-sig", "latin1"]
    last_err: Exception | None = None

    for enc in encodings:
        try:
            with open(csv_path, "r", encoding=enc, newline="") as f:
                first_line = f.readline()
                delim = ";" if ";" in first_line else ","
                f.seek(0)
                reader = csv.DictReader(f, delimiter=delim)
                for row in reader:
                    yield {str(k): (v if v is not None else "") for k, v in row.items() if k is not None}
            return
        except UnicodeDecodeError as e:
            last_err = e
            continue

    raise RuntimeError(f"Falha ao ler CSV (encoding). Último erro: {last_err}")


def _aggregate_basecompleta(csv_path: Path, month: str, resource_url: str) -> dict[str, Any]:
    """
    Agrega Base Completa (transacional) => estatísticas por empresa (nome).
    Filtro crítico: Segmento de Mercado == TARGET_SEGMENT.
    """
    target_norm = _norm(TARGET_SEGMENT)

    entries: dict[str, dict[str, Any]] = {}
    lines_total = 0
    lines_kept = 0

    # track display_name mais comum por key
    display_count: dict[str, dict[str, int]] = {}

    for row in _iter_rows(csv_path):
        lines_total += 1

        seg = _norm(row.get("Segmento de Mercado", ""))
        if seg != target_norm:
            continue

        name = (row.get("Nome Fantasia") or row.get("Empresa") or row.get("Fornecedor") or "").strip()
        if not name:
            continue

        situacao = _norm(row.get("Situação", ""))
        if "cancelada" in situacao:
            continue

        key = normalize_name_key(name)
        if not key:
            continue

        lines_kept += 1

        if key not in entries:
            entries[key] = {
                "name": name,
                "display_name": name,
                "cnpj": "",
                "statistics": {
                    "complaintsCount": 0,
                    "respondedCount": 0,
                    "resolvedCount": 0,
                    "finalizedCount": 0,
                    "scoreSum": 0.0,
                    "satisfactionCount": 0,
                    "total_claims": 0,
                    "responded_claims": 0,
                    "resolved_claims": 0,
                    "finalized_claims": 0,
                },
            }
            display_count[key] = {}

        dc = display_count[key]
        dc[name] = dc.get(name, 0) + 1
        if dc[name] > dc.get(entries[key]["display_name"], 0):
            entries[key]["display_name"] = name
            entries[key]["name"] = name

        st = entries[key]["statistics"]
        st["complaintsCount"] += 1
        st["total_claims"] += 1

        if _norm(row.get("Respondida", "")) == "s":
            st["respondedCount"] += 1
            st["responded_claims"] += 1

        if "finalizada" in situacao or "encerrada" in situacao:
            st["finalizedCount"] += 1
            st["finalized_claims"] += 1

        avaliacao = _norm(row.get("Avaliação Reclamação", ""))
        if avaliacao == "resolvida":
            st["resolvedCount"] += 1
            st["resolved_claims"] += 1

        nota_str = (row.get("Nota do Consumidor") or "").strip()
        if nota_str:
            nota = _safe_float(nota_str)
            if nota > 0:
                st["scoreSum"] += nota
                st["satisfactionCount"] += 1

    for e in entries.values():
        st = e.get("statistics") or {}
        sc = float(st.get("scoreSum") or 0.0)
        n = int(st.get("satisfactionCount") or 0)
        st["averageScore"] = round(sc / n, 4) if n > 0 else 0.0

    meta = {
        "status": "ok",
        "dataset": CG_DATASET_ID,
        "month": month,
        "source_file": str(csv_path),
        "resource_url": resource_url,
        "generated_at": _utc_now(),
        "filter_segment": TARGET_SEGMENT,
        "lines_total": lines_total,
        "lines_kept": lines_kept,
        "companies": len(entries),
    }

    return {
        "meta": meta,
        "by_name_key_raw": entries,
        "by_name_key": entries,
        "by_name": entries,
        "by_cnpj_key_raw": {},
        "by_cnpj_key": {},
    }


def _write_json_gz(obj: dict[str, Any], out_path_gz: Path) -> None:
    out_path_gz.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with gzip.open(out_path_gz, "wb") as f:
        f.write(payload)


def _merge_months(monthly: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, dict[str, Any]] = {}
    months: list[str] = []

    for root in monthly:
        meta = root.get("meta") or {}
        m = str(meta.get("month") or "")
        if m:
            months.append(m)

        by = root.get("by_name_key_raw") or {}
        if not isinstance(by, dict):
            continue

        for k, entry in by.items():
            if not isinstance(entry, dict):
                continue
            st = entry.get("statistics") or {}
            if k not in merged:
                merged[k] = {
                    "name": entry.get("name") or entry.get("display_name") or "",
                    "display_name": entry.get("display_name") or entry.get("name") or "",
                    "cnpj": entry.get("cnpj") or "",
                    "statistics": {
                        "complaintsCount": 0,
                        "respondedCount": 0,
                        "resolvedCount": 0,
                        "finalizedCount": 0,
                        "scoreSum": 0.0,
                        "satisfactionCount": 0,
                        "total_claims": 0,
                        "responded_claims": 0,
                        "resolved_claims": 0,
                        "finalized_claims": 0,
                    },
                }

            mst = merged[k]["statistics"]
            for fld in ("complaintsCount", "respondedCount", "resolvedCount", "finalizedCount", "satisfactionCount"):
                mst[fld] += int(st.get(fld) or 0)
            mst["scoreSum"] += float(st.get("scoreSum") or 0.0)
            for fld in ("total_claims", "responded_claims", "resolved_claims", "finalized_claims"):
                mst[fld] += int(st.get(fld) or 0)

    for e in merged.values():
        st = e["statistics"]
        sc = float(st.get("scoreSum") or 0.0)
        n = int(st.get("satisfactionCount") or 0)
        st["averageScore"] = round(sc / n, 4) if n > 0 else 0.0

    meta = {
        "status": "ok",
        "dataset": CG_DATASET_ID,
        "months": sorted(set(months)),
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "filter_segment": TARGET_SEGMENT,
        "companies": len(merged),
    }

    return {
        "meta": meta,
        "by_name_key_raw": merged,
        "by_name_key": merged,
        "by_name": merged,
        "by_cnpj_key_raw": {},
        "by_cnpj_key": {},
    }


def main() -> int:
    print("\n--- BUILD CONSUMIDOR.GOV (BASE COMPLETA -> AGG) ---")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    MONTHLY_DIR.mkdir(parents=True, exist_ok=True)
    DERIVED_DIR.mkdir(parents=True, exist_ok=True)

    try:
        res_by_month = _list_basecompleta_resources()
    except Exception as e:
        print(f"CG FAIL: não consegui ler dataset CKAN: {e}")
        return 1

    if not res_by_month:
        print("CG FAIL: nenhum recurso 'Base Completa' encontrado no dataset.")
        return 1

    available_months = sorted(res_by_month.keys())
    latest_month = available_months[-1]
    if FORCE_MONTH:
        if FORCE_MONTH not in res_by_month:
            print(f"CG FAIL: CG_FORCE_MONTH={FORCE_MONTH} não existe no dataset. Disponíveis: {available_months[-6:]}")
            return 1
        latest_month = FORCE_MONTH

    print(f"CG: mês mais recente disponível: {latest_month}")
    anchor = _month_to_date(latest_month)

    months: list[str] = []
    for i in range(MONTHS_BACK):
        m = _ym(_add_months(anchor, -i))
        if m in res_by_month:
            months.append(m)

    months = sorted(months, reverse=True)
    if not months:
        print("CG FAIL: nenhum mês processável encontrado após aplicar window.")
        return 1

    print(f"CG: meses a processar (até {MONTHS_BACK}): {months}")

    monthly_roots: list[dict[str, Any]] = []

    for m in months:
        res = res_by_month[m]
        out_gz = MONTHLY_DIR / f"consumidor_gov_{m}.json.gz"
        if out_gz.exists() and not FORCE_DOWNLOAD:
            print(f"CG: {m} já existe ({out_gz.name}), pulando (CG_FORCE_DOWNLOAD=1 para forçar).")
            try:
                with gzip.open(out_gz, "rb") as f:
                    monthly_roots.append(json.loads(f.read().decode("utf-8")))
            except Exception:
                print(f"CG: {m} arquivo existente corrompido, reprocessando.")
            else:
                continue

        raw_csv = RAW_DIR / f"basecompleta_{m}.csv"
        if FORCE_DOWNLOAD or (not raw_csv.exists()) or raw_csv.stat().st_size == 0:
            print(f"CG: baixando {m}: {res.name}")
            _download(res.url, raw_csv)
            print(f"CG: OK download {raw_csv.name} ({raw_csv.stat().st_size} bytes)")
        else:
            print(f"CG: usando cache {raw_csv.name} ({raw_csv.stat().st_size} bytes)")

        root = _aggregate_basecompleta(raw_csv, month=m, resource_url=res.url)
        _write_json_gz(root, out_gz)
        print(f"CG: OK agregado {m} -> {out_gz.as_posix()}")
        meta = root.get("meta") or {}
        print(f"CG:   linhas_total={meta.get('lines_total')} kept={meta.get('lines_kept')} empresas={meta.get('companies')}")
        monthly_roots.append(root)

    agg = _merge_months(monthly_roots)
    agg_gz = DERIVED_DIR / "consumidor_gov_agg.json.gz"
    _write_json_gz(agg, agg_gz)
    print(f"CG: OK agregado multi-mês -> {agg_gz.as_posix()} (empresas={agg.get('meta',{}).get('companies')})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
