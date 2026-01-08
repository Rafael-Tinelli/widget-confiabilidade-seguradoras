# api/sources/consumidor_gov_agg.py
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Tuple


# Onde fica o agregado final (opcionalmente já gerado)
AGG_FILE = Path(os.getenv("CG_AGG_FILE", "data/derived/consumidor_gov/aggregated.json"))

# Onde procurar os arquivos mensais consumidor_gov_YYYY-MM.json
# (vamos varrer recursivamente para não depender de layout fixo)
MONTHLY_ROOT = Path(os.getenv("CG_MONTHLY_ROOT", "data/derived/consumidor_gov"))

# Se não existir agregado, pode gerar e salvar automaticamente
WRITE_AGG_IF_MISSING = os.getenv("CG_WRITE_AGG_IF_MISSING", "1") == "1"

_YM_RE = re.compile(r"consumidor_gov_(20\d{2}-[01]\d)\.json$", re.I)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _iter_monthly_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    files = []
    for p in root.rglob("consumidor_gov_*.json"):
        if p.name.lower() == "aggregated.json":
            continue
        if _YM_RE.search(p.name):
            files.append(p)
    # Ordena por nome (YYYY-MM) para determinismo
    files.sort(key=lambda x: x.name)
    return files


def _merge_entry(dst: dict[str, Any], src: dict[str, Any]) -> None:
    """
    Mescla estatísticas "por soma" e recalcula médias/índices no final.
    Espera o formato do seu monthly:
      entry["statistics"] com counts e também scoreSum/responseTimeSum.
    """
    ds = dst.setdefault("statistics", {})
    ss = (src.get("statistics") or {})

    # contagens
    ds["complaintsCount"] = int(ds.get("complaintsCount") or 0) + int(ss.get("complaintsCount") or 0)
    ds["finalizedCount"] = int(ds.get("finalizedCount") or 0) + int(ss.get("finalizedCount") or 0)
    ds["evaluatedCount"] = int(ds.get("evaluatedCount") or 0) + int(ss.get("evaluatedCount") or 0)
    ds["respondedCount"] = int(ds.get("respondedCount") or 0) + int(ss.get("respondedCount") or 0)
    ds["resolvedCount"] = int(ds.get("resolvedCount") or 0) + int(ss.get("resolvedCount") or 0)

    # somas brutas (para médias)
    ds["scoreSum"] = float(ds.get("scoreSum") or 0.0) + float(ss.get("scoreSum") or 0.0)
    ds["responseTimeSum"] = float(ds.get("responseTimeSum") or 0.0) + float(ss.get("responseTimeSum") or 0.0)
    ds["responseTimeCount"] = int(ds.get("responseTimeCount") or 0) + int(ss.get("responseTimeCount") or 0)

    # cnpj: preserva o primeiro útil
    if not dst.get("cnpj") and src.get("cnpj"):
        dst["cnpj"] = src.get("cnpj")

    # display_name: tenta manter o mais “informativo”
    dn_dst = (dst.get("display_name") or dst.get("name") or "").strip()
    dn_src = (src.get("display_name") or src.get("name") or "").strip()
    if dn_src and (not dn_dst or len(dn_src) > len(dn_dst)):
        dst["display_name"] = dn_src
        dst["name"] = dn_src  # compat


def _finalize_entry(entry: dict[str, Any]) -> None:
    st = entry.get("statistics") or {}
    ec = int(st.get("evaluatedCount") or 0)
    tc = int(st.get("complaintsCount") or 0)
    fin = int(st.get("finalizedCount") or 0)
    resolved = int(st.get("resolvedCount") or 0)

    score_sum = float(st.get("scoreSum") or 0.0)
    rt_sum = float(st.get("responseTimeSum") or 0.0)
    rt_count = int(st.get("responseTimeCount") or 0)

    overall = round(score_sum / ec, 2) if ec > 0 else None

    denom_sol = ec if ec > 0 else (fin if fin > 0 else tc)
    sol_idx = round(resolved / denom_sol, 2) if denom_sol > 0 else None

    avg_rt = round(rt_sum / rt_count, 1) if rt_count > 0 else None

    st["overallSatisfaction"] = overall
    st["solutionIndex"] = sol_idx
    st["averageResponseTime"] = avg_rt

    # compat com seu formato anterior
    entry.setdefault("indexes", {})
    entry["indexes"].setdefault("b", {})
    entry["indexes"]["b"]["nota"] = overall


def _build_aggregated_from_monthlies(monthlies: list[Path]) -> dict[str, Any]:
    by_name: dict[str, dict[str, Any]] = {}
    by_cnpj: dict[str, dict[str, Any]] = {}

    yms: list[str] = []

    for p in monthlies:
        m = _YM_RE.search(p.name)
        if m:
            yms.append(m.group(1))

        payload = _safe_read_json(p) or {}
        entries = payload.get("by_name_key_raw") or payload.get("by_name") or {}
        if not isinstance(entries, dict):
            continue

        for k, entry in entries.items():
            if not isinstance(entry, dict):
                continue

            # merge por name-key
            if k not in by_name:
                # shallow copy
                by_name[k] = {
                    "display_name": entry.get("display_name") or entry.get("name") or "",
                    "name": entry.get("display_name") or entry.get("name") or "",
                    "cnpj": entry.get("cnpj"),
                    "statistics": dict(entry.get("statistics") or {}),
                    "indexes": dict(entry.get("indexes") or {}),
                }
            else:
                _merge_entry(by_name[k], entry)

    # finaliza e monta índice por cnpj
    for k, entry in by_name.items():
        _finalize_entry(entry)
        cnpj = (entry.get("cnpj") or "").strip()
        if cnpj:
            by_cnpj[cnpj] = entry

    out = {
        "meta": {
            "generated_at": _utc_now(),
            "months": yms,
            "monthly_files": len(monthlies),
            "source": "built_from_monthlies",
        },
        "by_name_key_raw": by_name,
        "by_cnpj_key_raw": by_cnpj,
    }
    return out


def extract_consumidor_gov_aggregated() -> Tuple[dict[str, Any], dict[str, Any]]:
    """
    Retorna (meta, payload) para o build.
    payload deve conter by_name_key_raw/by_cnpj_key_raw para o NameMatcher.
    """
    # 1) Se já existe agregado, usa-o
    if AGG_FILE.exists():
        data = _safe_read_json(AGG_FILE)
        if isinstance(data, dict) and (data.get("by_name_key_raw") or data.get("by_name")):
            meta = data.get("meta") or {}
            meta = {
                **(meta if isinstance(meta, dict) else {}),
                "status": "loaded_aggregated",
                "path": str(AGG_FILE),
            }
            return meta, data

    # 2) Senão, tenta agregar a partir dos mensais
    monthlies = _iter_monthly_files(MONTHLY_ROOT)
    if not monthlies:
        meta = {
            "status": "missing",
            "understood": "no aggregated file and no monthly files found",
            "agg_path": str(AGG_FILE),
            "monthly_root": str(MONTHLY_ROOT),
            "generated_at": _utc_now(),
        }
        return meta, {}

    data = _build_aggregated_from_monthlies(monthlies)

    if WRITE_AGG_IF_MISSING:
        try:
            AGG_FILE.parent.mkdir(parents=True, exist_ok=True)
            AGG_FILE.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        except Exception:
            # best-effort: não quebra pipeline
            pass

    meta = data.get("meta") or {}
    meta = {
        **(meta if isinstance(meta, dict) else {}),
        "status": "built_from_monthlies",
        "agg_path": str(AGG_FILE),
        "monthly_root": str(MONTHLY_ROOT),
    }
    return meta, data
